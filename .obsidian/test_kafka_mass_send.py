"""
Тест: Массовая отправка сообщений в Kafka и проверка ответов.

Шаблоны запросов лежат отдельными файлами в папке requests_kafka (рядом с конфигом).
В kafka_mass_send_config.json перечисляется, какие запросы и сколько раз отправить,
ЛС для каждого сообщения выбирается случайно из accounts.
Путь к конфигу можно переопределить переменной окружения KAFKA_MASS_SEND_CONFIG.
Ответ ищется в топике ответов по ключу сообщения (ЛС) и correlationId.
"""
import calendar
import json
import os
import random
import re
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import allure
from confluent_kafka import KafkaError, KafkaException, OFFSET_END
from tests.api.rest.kafka_helper import create_kafka_consumer, create_kafka_producer


CONFIG_PATH = os.environ.get('KAFKA_MASS_SEND_CONFIG') or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'kafka_mass_send_config.json')

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
                                config.get('run_counter_file', 'kafka_run_counter.txt'))
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


# @allure.id("")  # TODO: указать номер теста из TestOps
@allure.title("Массовая отправка сообщений в Kafka и проверка ответов")
@allure.suite("BP Autotests")
@allure.epic("OpenAPI")
@allure.label("Вид тестирования", "Регресс")
@allure.label("Статус теста", "Актуален")
@allure.label("Уровень теста", "API")
@allure.label("Приоритет теста", "Средний")
@allure.label("Статус автоматизации", "Автоматизирован")
def test_kafka_mass_send():
    """Отправляет запросы из requests_kafka на случайные ЛС и проверяет ответы в топике ответов."""
    config = load_config()
    requests = load_requests(config)
    topic = config['topic']
    accounts = [str(a) for a in config['accounts']]
    total = sum(r['count'] for r in requests)
    seed = config.get('random_seed')
    if seed is None:
        seed = random.randrange(10 ** 9)
    rng = random.Random(seed)
    run_number = next_run_number(config)
    response_cfg = config['response']
    response_topic = response_cfg['topic']
    corr_field = response_cfg.get('correlation_id_field', 'data.correlationId')
    tz = timezone(timedelta(hours=config.get('utc_offset_hours', 3)))
    headers = list((config.get('headers') or {}).items()) or None
    attach_limit = int(config.get('allure_attach_limit', 20))

    _attach_json(dict(config, random_seed_used=seed, run_number=run_number), "Конфиг")
    print(f"\n[MassSend] Топик: {topic}, ЛС в пуле: {len(accounts)}, всего сообщений: {total}, "
          f"random_seed: {seed}, номер запуска: {run_number}")
    for r in requests:
        print(f"[MassSend]   запрос {r['file']}: {r['count']} шт.")

    consumer = None
    producer = None
    try:
        with allure.step(f"Подписка на топик ответов {response_topic}"):
            consumer = create_kafka_consumer(topic=response_topic, on_assign=_start_from_end)
            _wait_for_assignment(consumer)

        sent = {}
        delivered = set()
        delivery_errors = {}

        with allure.step(f"Отправка {total} сообщений в топик {topic}"):
            producer = create_kafka_producer(config.get('producer_config'))

            def on_delivery_factory(pair):
                def on_delivery(err, _msg):
                    if err is not None:
                        delivery_errors[pair] = str(err)
                    else:
                        delivered.add(pair)
                return on_delivery

            started = time.time()
            index = 0
            examples = {}
            for request in requests:
                for _ in range(request['count']):
                    index += 1
                    account = rng.choice(accounts)
                    message = render_template(request['template'], account, index, tz, run_number)
                    key = build_key(config, message, account, index, tz, run_number)
                    corr_id = str(get_by_path(message, corr_field))
                    pair = (key, corr_id)
                    assert pair not in sent, (
                        f"Повтор ключа и correlationId {pair} в {request['file']} — "
                        f"используйте {{uuid}} в {corr_field}")
                    sent[pair] = {'request': request['file'], 'account': account,
                                  'response_key': build_response_key(config, message, key)}
                    examples.setdefault(request['file'], message)

                    _produce(
                        producer,
                        topic=topic,
                        key=key.encode('utf-8'),
                        value=json.dumps(message, ensure_ascii=False).encode('utf-8'),
                        headers=headers,
                        on_delivery=on_delivery_factory(pair),
                    )

            not_flushed = producer.flush(config.get('flush_timeout_seconds', 120))
            elapsed = time.time() - started
            failed = total - len(delivered)
            fail_percent = failed / total * 100

            print(f"[MassSend] Доставлено: {len(delivered)}/{total} за {elapsed:.1f}с "
                  f"({total / max(elapsed, 0.001):.0f} msg/s), ошибок: {len(delivery_errors)}, "
                  f"без подтверждения: {not_flushed}")
            for file_name, example in examples.items():
                _attach_json(example, f"Пример сообщения: {file_name}")
            _attach_json({
                'random_seed': seed,
                'по запросам': dict(Counter(v['request'] for v in sent.values())),
                'по ЛС': dict(Counter(v['account'] for v in sent.values())),
            }, "Распределение сообщений")
            _attach_json({
                'total': total,
                'delivered': len(delivered),
                'errors': len(delivery_errors),
                'not_flushed': not_flushed,
                'fail_percent': round(fail_percent, 2),
                'elapsed_seconds': round(elapsed, 2),
                'error_examples': [
                    {'request': sent[(k, c)]['request'], 'key': k, 'correlationId': c, 'error': e}
                    for (k, c), e in list(delivery_errors.items())[:attach_limit]
                ],
            }, "Итог доставки")

            max_fail = config.get('max_delivery_fail_percent', 0)
            assert fail_percent <= max_fail, (
                f"Не доставлено {failed} из {total} ({fail_percent:.2f}%), допустимо {max_fail}%")

        with allure.step(f"Ожидание ответов в топике {response_topic} "
                         f"({response_cfg.get('timeout_seconds', 60)}с)"):
            pending_by_corr = {_normalize(c): (k, c) for k, c in delivered}
            found = {}
            key_mismatches = []
            unmatched = []
            checked = 0
            timeout = response_cfg.get('timeout_seconds', 60)
            deadline = time.time() + timeout

            while pending_by_corr and time.time() < deadline:
                msg = consumer.poll(timeout=min(5.0, max(0.1, deadline - time.time())))
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[MassSend] ⚠️ {msg.error()}")
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
                matched_corr = next((c for c in candidates if c in pending_by_corr), None)

                diag = {
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': key,
                    f'{corr_field} (по пути)': corr_by_path,
                    'headers': headers_dict,
                    'value_preview': text[:1000],
                }

                if matched_corr is None:
                    if len(unmatched) < attach_limit:
                        unmatched.append(diag)
                        print(f"[MassSend] ✖ Не наше/не сопоставлено: key={key!r}, "
                              f"{corr_field}={corr_by_path!r}, headers={headers_dict}, "
                              f"value[:300]={text[:300]!r}")
                    continue

                sent_key, expected_corr = pending_by_corr[matched_corr]
                expected_key = sent[(sent_key, expected_corr)]['response_key']
                if _normalize(key) != _normalize(expected_key):
                    diag['expected_key'] = expected_key
                    diag['expected_correlationId'] = expected_corr
                    key_mismatches.append(diag)
                    print(f"[MassSend] ⚠️ correlationId {expected_corr} совпал, но ключ другой: "
                          f"ожидали {expected_key!r}, пришёл {key!r}")
                    continue

                del pending_by_corr[matched_corr]
                found[(sent_key, expected_corr)] = parsed
                print(f"[MassSend] ✅ Ответ: key={key}, correlationId={expected_corr}")

            pending = list(pending_by_corr.values())
            missing_percent = len(pending) / len(delivered) * 100 if delivered else 0
            print(f"[MassSend] Ответов найдено: {len(found)}/{len(delivered)}, "
                  f"проверено сообщений в топике: {checked}, без ответа: {len(pending)}, "
                  f"совпал correlationId, но не ключ: {len(key_mismatches)}")

            _attach_json([
                {'request': sent[(k, c)]['request'], 'key': k, 'correlationId': c, 'response': r}
                for (k, c), r in list(found.items())[:attach_limit]
            ], f"Ответы (первые {attach_limit})")
            if pending:
                print(f"[MassSend] Без ответа по запросам: "
                      f"{dict(Counter(sent[p]['request'] for p in pending))}")
                _attach_json([
                    {'request': sent[(k, c)]['request'], 'key': k, 'correlationId': c}
                    for k, c in pending[:attach_limit]
                ], f"Сообщения без ответа (первые {attach_limit})")
            if key_mismatches:
                _attach_json(key_mismatches[:attach_limit],
                             "Совпал correlationId, но ключ другой")
            if unmatched:
                _attach_json(unmatched, f"Несопоставленные сообщения из топика ответов (первые {attach_limit})")

            max_missing = response_cfg.get('max_missing_percent', 0)
            assert missing_percent <= max_missing, (
                f"Нет ответа на {len(pending)} из {len(delivered)} сообщений "
                f"({missing_percent:.2f}%) за {timeout}с, допустимо {max_missing}%. "
                f"Проверено сообщений в топике: {checked}, из них с совпавшим correlationId, "
                f"но другим ключом: {len(key_mismatches)}, несопоставленных: {len(unmatched)} "
                f"(подробности в логе и allure)")

    finally:
        if producer is not None:
            producer.flush(10)
        if consumer is not None:
            consumer.close()
            print("[MassSend] Consumer закрыт")
