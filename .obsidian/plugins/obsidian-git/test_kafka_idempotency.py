"""
Тест: Проверка идемпотентности — повторная отправка одинаковых сообщений в Kafka.

Каждое сообщение отправляется один раз, тест дожидается ответа на него, после чего отправляет
побайтово такое же сообщение (тот же ключ, id, correlationId, externalId, даты) ещё repeats - 1 раз.
Ожидается, что на повторы сервис больше не ответит: ровно один ответ на сообщение.

Шаблоны запросов берутся из папки requests_kafka (общая с тестом массовой отправки),
настройки — из kafka_idempotency_config.json (путь можно переопределить переменной окружения
KAFKA_IDEMPOTENCY_CONFIG). ЛС берутся из accounts по очереди, у каждого запроса с первого ЛС.
"""
import calendar
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import allure
from confluent_kafka import KafkaError, KafkaException, OFFSET_END
from tests.api.rest.kafka_helper import create_kafka_consumer, create_kafka_producer


CONFIG_PATH = os.environ.get('KAFKA_IDEMPOTENCY_CONFIG') or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'kafka_idempotency_config.json')

PLACEHOLDER_RE = re.compile(r'\{(account|uuid|index|run|now|end_of_month)\}')
UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
CHARGES_TOTAL_PLACEHOLDER = '{charges_total}'

REQUIRED_KEYS = ('topic', 'accounts', 'requests', 'response')


def load_config(path=CONFIG_PATH):
    with open(path, encoding='utf-8') as f:
        config = json.load(f)

    missing = [k for k in REQUIRED_KEYS if k not in config]
    if missing:
        raise ValueError(f"В конфиге {path} нет обязательных полей: {missing}")
    if not config['accounts']:
        raise ValueError(f"Массив accounts в конфиге {path} пуст — добавьте ЛС")
    if not config['requests']:
        raise ValueError(f"Массив requests в конфиге {path} пуст — укажите хотя бы один запрос")
    if not config.get('key_template') and not config.get('key_field'):
        raise ValueError("Укажите key_field или key_template")
    if '{key_field}' in (config.get('key_template') or '') and not config.get('key_field'):
        raise ValueError("key_template содержит {key_field}, но key_field не задан")
    if int(config.get('repeats', 2)) < 2:
        raise ValueError("repeats должен быть не меньше 2: оригинал + хотя бы один повтор")
    return config


def load_requests(config, config_path=CONFIG_PATH):
    """Читает шаблоны запросов из папки requests_dir: [{'file', 'count', 'template'}, ...]."""
    requests_dir = os.path.join(os.path.dirname(os.path.abspath(config_path)),
                                config.get('requests_dir', 'requests_kafka'))
    requests = []
    for item in config['requests']:
        file_name = item['file']
        count = int(item.get('count', 0))
        if count <= 0:
            raise ValueError(f"У запроса {file_name} count должен быть больше 0")
        file_path = os.path.join(requests_dir, file_name)
        if not os.path.exists(file_path):
            available = sorted(os.listdir(requests_dir)) if os.path.isdir(requests_dir) else None
            raise ValueError(
                f"Файл запроса не найден: {file_path}. "
                + (f"Папки {requests_dir} нет — проверьте, что она закоммичена"
                   if available is None else f"Файлы в папке: {available}"))
        with open(file_path, encoding='utf-8') as f:
            requests.append({'file': file_name, 'count': count, 'template': json.load(f)})
    return requests


def get_by_path(obj, path):
    """Достаёт значение по пути через точку: data.billingAccountId, data.charges.0.externalId."""
    for part in path.split('.'):
        obj = obj[int(part)] if isinstance(obj, list) else obj[part]
    return obj


def next_run_number(config, config_path=CONFIG_PATH):
    """Номер запуска: в GitLab — номер пайплайна (CI_PIPELINE_IID), локально — счётчик в файле +1."""
    ci_number = os.environ.get('CI_PIPELINE_IID')
    if ci_number:
        return int(ci_number)

    counter_path = os.path.join(os.path.dirname(os.path.abspath(config_path)),
                                config.get('run_counter_file', 'kafka_idempotency_run_counter.txt'))
    try:
        with open(counter_path, encoding='utf-8') as f:
            last = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        last = 0
    number = last + 1
    with open(counter_path, 'w', encoding='utf-8') as f:
        f.write(str(number))
    return number


def render_template(template, account, index, tz, run=''):
    """Рекурсивно подставляет плейсхолдеры во все строки шаблона."""
    now = datetime.now(tz)
    last_day = calendar.monthrange(now.year, now.month)[1]
    end_of_month = now.replace(day=last_day, hour=23, minute=59, second=59, microsecond=0)
    values = {
        'account': str(account),
        'index': str(index),
        'run': str(run),
        'now': now.isoformat(timespec='milliseconds'),
        'end_of_month': end_of_month.isoformat(timespec='milliseconds'),
    }

    def substitute(match):
        name = match.group(1)
        return str(uuid.uuid4()) if name == 'uuid' else values[name]

    def walk(node):
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        if isinstance(node, str):
            return PLACEHOLDER_RE.sub(substitute, node)
        return node

    return _fill_charges_total(walk(template))


def _fill_charges_total(message):
    """Заменяет значение "{charges_total}" на сумму data.charges[].totalAmount.amount (числом)."""
    total = []

    def walk(node):
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        if node == CHARGES_TOTAL_PLACEHOLDER:
            if not total:
                charges = get_by_path(message, 'data.charges')
                total.append(float(sum(
                    (Decimal(str(c['totalAmount']['amount'])) for c in charges), Decimal('0'))))
            return total[0]
        return node

    return walk(message)


def build_key(config, message, account, index, tz, run=''):
    """key_template (если задан) с подстановкой {key_field} — значения из сообщения по key_field."""
    field_value = str(get_by_path(message, config['key_field'])) if config.get('key_field') else ''
    template = config.get('key_template')
    if not template:
        return field_value
    return render_template(template.replace('{key_field}', field_value), account, index, tz, run)


def build_response_key(config, message, sent_key):
    """Ожидаемый ключ ответа: значение из отправленного сообщения по response.key_field, иначе ключ отправки."""
    field = config['response'].get('key_field')
    return str(get_by_path(message, field)) if field else sent_key


def _start_from_end(consumer, partitions):
    """Фиксирует позицию чтения на текущем конце партиций, чтобы не пропустить ответы на наши сообщения."""
    for p in partitions:
        try:
            _, high = consumer.get_watermark_offsets(p, timeout=10)
            p.offset = high
        except KafkaException:
            p.offset = OFFSET_END
    consumer.assign(partitions)


def _wait_for_assignment(consumer, timeout_seconds=60):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        consumer.poll(timeout=1.0)
        assignment = consumer.assignment()
        if assignment:
            print(f"[Kafka] Назначены партиции: {[p.partition for p in assignment]}")
            return assignment
    raise AssertionError(f"Consumer не получил партиции за {timeout_seconds}с")


def _produce(producer, **kwargs):
    while True:
        try:
            producer.produce(**kwargs)
            break
        except BufferError:
            producer.poll(1)
    producer.poll(0)


def _normalize(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode('utf-8', errors='replace')
    return str(value).strip().strip('"').strip().lower()


def _decode_headers(msg):
    return {
        k: (v.decode('utf-8', errors='replace') if isinstance(v, bytes) else v)
        for k, v in (msg.headers() or [])
    }


def _attach_json(data, name):
    allure.attach(json.dumps(data, ensure_ascii=False, indent=2),
                  name=name, attachment_type=allure.attachment_type.JSON)


def _send_batch(producer, topic, items, headers, flush_timeout):
    """Отправляет items (key/value в байтах) и ждёт подтверждений. Возвращает список ошибок доставки."""
    errors = []
    delivered = []

    def on_delivery_factory(item):
        def on_delivery(err, _msg):
            if err is not None:
                errors.append({'key': item['key'], 'correlationId': item['corr'], 'error': str(err)})
            else:
                delivered.append(item['corr'])
        return on_delivery

    for item in items:
        _produce(producer, topic=topic, key=item['key_bytes'], value=item['value_bytes'],
                 headers=headers, on_delivery=on_delivery_factory(item))
    not_flushed = producer.flush(flush_timeout)
    if not_flushed:
        errors.append({'error': f"Без подтверждения брокера осталось {not_flushed} сообщений"})
    print(f"[Idempotency] Доставлено: {len(delivered)}/{len(items)}, ошибок: {len(errors)}")
    return errors


def _collect_responses(consumer, expected, corr_field, timeout_seconds, stop_when_all_found, log_limit):
    """
    Читает топик ответов и раскладывает ответы по correlationId из expected.
    expected: {нормализованный correlationId: {'corr', 'response_key', ...}}.
    stop_when_all_found=True — остановиться, как только на каждое сообщение есть хотя бы один ответ;
    False — слушать весь timeout_seconds (нужно, чтобы убедиться, что лишних ответов нет).
    """
    responses = {c: [] for c in expected}
    key_mismatches = {c: [] for c in expected}
    unmatched = []
    checked = 0
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        if stop_when_all_found and all(responses.values()):
            break
        msg = consumer.poll(timeout=min(5.0, max(0.1, deadline - time.time())))
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[Idempotency] ⚠️ {msg.error()}")
            continue

        checked += 1
        key = msg.key().decode('utf-8', errors='replace') if msg.key() else None
        text = msg.value().decode('utf-8', errors='replace') if msg.value() else ''
        headers_dict = _decode_headers(msg)
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = {'raw': text}
        try:
            corr_by_path = get_by_path(parsed, corr_field)
        except (KeyError, IndexError, TypeError, ValueError):
            corr_by_path = None

        candidates = UUID_RE.findall((text + ' ' + json.dumps(headers_dict)).lower())
        candidates.insert(0, _normalize(corr_by_path))
        matched_corr = next((c for c in candidates if c in expected), None)

        diag = {
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': key,
            f'{corr_field} (по пути)': corr_by_path,
            'headers': headers_dict,
            'value_preview': text[:1000],
        }

        if matched_corr is None:
            if len(unmatched) < log_limit:
                unmatched.append(diag)
                print(f"[Idempotency] ✖ Не наше/не сопоставлено: key={key!r}, "
                      f"{corr_field}={corr_by_path!r}, value[:300]={text[:300]!r}")
            continue

        exp = expected[matched_corr]
        if _normalize(key) != _normalize(exp['response_key']):
            diag['expected_key'] = exp['response_key']
            key_mismatches[matched_corr].append(diag)
            print(f"[Idempotency] ⚠️ correlationId {exp['corr']} совпал, но ключ другой: "
                  f"ожидали {exp['response_key']!r}, пришёл {key!r}")
            continue

        responses[matched_corr].append(parsed)
        print(f"[Idempotency] ✅ Ответ №{len(responses[matched_corr])}: key={key}, "
              f"correlationId={exp['corr']}")

    return responses, key_mismatches, unmatched, checked


# @allure.id("")  # TODO: указать номер теста из TestOps
@allure.title("Проверка идемпотентности: повторная отправка одинаковых сообщений в Kafka")
@allure.suite("BP Autotests")
@allure.epic("OpenAPI")
@allure.label("Вид тестирования", "Регресс")
@allure.label("Статус теста", "Актуален")
@allure.label("Уровень теста", "API")
@allure.label("Приоритет теста", "Средний")
@allure.label("Статус автоматизации", "Автоматизирован")
def test_kafka_idempotency():
    """Отправляет сообщение, ждёт ответ, повторяет его побайтово и проверяет, что второго ответа нет."""
    config = load_config()
    requests = load_requests(config)
    topic = config['topic']
    accounts = [str(a) for a in config['accounts']]
    repeats = int(config.get('repeats', 2))
    run_number = next_run_number(config)
    response_cfg = config['response']
    response_topic = response_cfg['topic']
    corr_field = response_cfg.get('correlation_id_field', 'data.correlationId')
    response_timeout = response_cfg.get('timeout_seconds', 60)
    duplicate_wait = response_cfg.get('duplicate_wait_seconds', 60)
    flush_timeout = config.get('flush_timeout_seconds', 120)
    tz = timezone(timedelta(hours=config.get('utc_offset_hours', 3)))
    headers = list((config.get('headers') or {}).items()) or None
    attach_limit = int(config.get('allure_attach_limit', 20))

    _attach_json(dict(config, run_number=run_number), "Конфиг")
    total = sum(r['count'] for r in requests)
    print(f"\n[Idempotency] Топик: {topic}, сообщений: {total}, каждое отправляется {repeats} раз(а), "
          f"номер запуска: {run_number}")

    consumer = None
    producer = None
    try:
        with allure.step(f"Подписка на топик ответов {response_topic}"):
            consumer = create_kafka_consumer(topic=response_topic, on_assign=_start_from_end)
            _wait_for_assignment(consumer)

        with allure.step("Подготовка сообщений"):
            originals = []
            index = 0
            for request in requests:
                for position in range(request['count']):
                    index += 1
                    account = accounts[position % len(accounts)]
                    message = render_template(request['template'], account, index, tz, run_number)
                    key = build_key(config, message, account, index, tz, run_number)
                    corr = str(get_by_path(message, corr_field))
                    originals.append({
                        'request': request['file'],
                        'account': account,
                        'key': key,
                        'corr': corr,
                        'response_key': build_response_key(config, message, key),
                        'key_bytes': key.encode('utf-8'),
                        'value_bytes': json.dumps(message, ensure_ascii=False).encode('utf-8'),
                        'message': message,
                    })
            expected = {_normalize(o['corr']): o for o in originals}
            assert len(expected) == len(originals), (
                f"correlationId повторяются между сообщениями — используйте {{uuid}} в {corr_field}")
            for o in originals[:attach_limit]:
                _attach_json(o['message'], f"Сообщение {o['request']} → {o['key']}")

        with allure.step(f"Отправка оригиналов ({len(originals)} шт.) в {topic}"):
            producer = create_kafka_producer(config.get('producer_config'))
            errors = _send_batch(producer, topic, originals, headers, flush_timeout)
            if errors:
                _attach_json(errors[:attach_limit], "Ошибки доставки оригиналов")
            assert not errors, f"Не все оригиналы доставлены: {errors[:5]}"

        with allure.step(f"Ожидание ответов на оригиналы ({response_timeout}с)"):
            first, first_mismatch, first_unmatched, first_checked = _collect_responses(
                consumer, expected, corr_field, response_timeout,
                stop_when_all_found=True, log_limit=attach_limit)
            without_response = [expected[c] for c, r in first.items() if not r]
            print(f"[Idempotency] Ответов на оригиналы: {len(originals) - len(without_response)}"
                  f"/{len(originals)}, проверено сообщений в топике: {first_checked}")
            _attach_json([
                {'request': expected[c]['request'], 'key': expected[c]['key'],
                 'correlationId': expected[c]['corr'], 'responses': r}
                for c, r in list(first.items())[:attach_limit]
            ], "Ответы на оригиналы")
            if first_unmatched:
                _attach_json(first_unmatched, "Несопоставленные сообщения (оригиналы)")
            if any(first_mismatch.values()):
                _attach_json({expected[c]['corr']: d for c, d in first_mismatch.items() if d},
                             "Совпал correlationId, но ключ другой (оригиналы)")
            assert not without_response, (
                f"Нет ответа на {len(without_response)} из {len(originals)} оригиналов за "
                f"{response_timeout}с — проверить идемпотентность невозможно: "
                f"{[(o['key'], o['corr']) for o in without_response[:5]]}")

        with allure.step(f"Повторная отправка тех же сообщений ({repeats - 1} раз(а))"):
            for attempt in range(1, repeats):
                print(f"[Idempotency] Повтор {attempt}/{repeats - 1}")
                errors = _send_batch(producer, topic, originals, headers, flush_timeout)
                if errors:
                    _attach_json(errors[:attach_limit], f"Ошибки доставки повтора {attempt}")
                assert not errors, f"Не все повторы доставлены (повтор {attempt}): {errors[:5]}"

        with allure.step(f"Проверка, что на повторы нет новых ответов ({duplicate_wait}с)"):
            extra, extra_mismatch, extra_unmatched, extra_checked = _collect_responses(
                consumer, expected, corr_field, duplicate_wait,
                stop_when_all_found=False, log_limit=attach_limit)

            summary = []
            violations = []
            for c, o in expected.items():
                row = {
                    'request': o['request'],
                    'account': o['account'],
                    'key': o['key'],
                    'correlationId': o['corr'],
                    'ответов на оригинал': len(first[c]),
                    'ответов после повтора': len(extra[c]),
                    'с другим ключом после повтора': len(extra_mismatch[c]),
                }
                summary.append(row)
                if len(first[c]) + len(extra[c]) + len(extra_mismatch[c]) != 1:
                    violations.append(row)

            print(f"[Idempotency] Проверено сообщений в топике после повтора: {extra_checked}, "
                  f"нарушений идемпотентности: {len(violations)}/{len(expected)}")
            _attach_json(summary[:attach_limit], "Итог по сообщениям")
            if any(extra.values()):
                _attach_json({expected[c]['corr']: r for c, r in extra.items() if r},
                             "Лишние ответы после повтора")
            if any(extra_mismatch.values()):
                _attach_json({expected[c]['corr']: d for c, d in extra_mismatch.items() if d},
                             "Совпал correlationId, но ключ другой (после повтора)")
            if extra_unmatched:
                _attach_json(extra_unmatched, "Несопоставленные сообщения (после повтора)")

            assert not violations, (
                f"Идемпотентность нарушена для {len(violations)} из {len(expected)} сообщений: "
                f"ожидался ровно 1 ответ. Примеры: {violations[:5]}")

    finally:
        if producer is not None:
            producer.flush(10)
        if consumer is not None:
            consumer.close()
            print("[Idempotency] Consumer закрыт")
