{
  "id": "{uuid}",
  "source": "/ep/charging/",
  "specversion": "1.0",
  "type": "ru.mts.ep.billing.v1.charges.created",
  "datacontenttype": "application/json",
  "dataschema": null,
  "subject": "{account}",
  "time": "{now}",
  "data": {
    "correlationId": "{uuid}",
    "billingAccountId": "{account}",
    "chargeDate": "{now}",
    "eventDate": "{now}",
    "charges": [
      {
        "externalId": "{uuid}",
        "chargeType": "SUBSCRIPTION",
        "totalAmount": {
          "amount": 6.00,
          "currency": "RUB"
        },
        "chargeContext": {
          "action": "PROLONGATE",
          "productId": "1f093b2d-36ed-61c8-aec6-fa5ecb780809",
          "quantity": {
            "amount": 1,
            "unit": "MONTH"
          },
          "pricePerUnit": {
            "amount": 6.00,
            "currency": "RUB"
          },
          "forPeriod": {
            "from": "{now}",
            "to": "{end_of_month}"
          },
          "optionalAttributes": [
            {
              "category": "ProductSpecification",
              "key": "productSpecificationId",
              "value": "6b950529-4d7f-41ba-a149-ed7e1e62a5dc"
            },
            {
              "category": "ProductSpecification",
              "key": "productSpecificationVersion",
              "value": "1"
            }
          ]
        }
      }
    ],
    "payments": [
      {
        "amount": {
          "amount": "{charges_total}",
          "currency": "RUB"
        },
        "methodType": "PERSONAL_ACCOUNT",
        "paymentDate": "{now}",
        "optionalAttributes": [
          {
            "key": "balanceAmount",
            "value": "1311.16"
          }
        ]
      }
    ]
  }
}
