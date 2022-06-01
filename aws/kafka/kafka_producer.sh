## produce for AWS kafka
# https://docs.aws.amazon.com/msk/latest/developerguide/produce-consume.html
cd /home/ec2-user/pgm/kafka_2.12-2.6.2/bin
./kafka-console-producer.sh --broker-list b-1.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092 --topic mk-topic-1
{"appsflyer_id": "13", "eventTime": "2022-05-12 14:37:22", "advertising_id": "38412345-8cf0-aa78-b23e-10b96e40000d", "eventName": "demo_test_af", "eventValue": {"af_order_id": "order_id", "af_price": "6", "af_revenue": "6", "af_currency": "USD", "af_content_type": "1m", "af_subscription_id": "af_subscription_id", "af_quantity": "1", "af_description": "af_description"}, "eventCurrency": "USD", "ip": "1.2.3.4", "language": "\u4e2d\u6587", "lang_code": "en-us", "platform": "ios", "customer_user_id": "uid", "af_param_1": "0"}
(ctrl+z to exit)
