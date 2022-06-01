# install s3://mk-glue-367793726391/lib/wheel/kafka_python-2.0.2-py2.py3-none-any.whl
# glue: setup kafka connection, and ingest into Additional network connections

from kafka import KafkaProducer
from time import sleep

def start_producer():
    bootstrap_servers=['b-1.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092']
    producer=KafkaProducer(bootstrap_servers=bootstrap_servers)
    topicName='mk-topic-1'
    mesg=r'{"appsflyer_id": "32", "eventTime": "2022-05-12 14:37:22", "advertising_id": "38412345-8cf0-aa78-b23e-10b96e40000d", "eventName": "demo_test_af", "eventValue": {"af_order_id": "order_id", "af_price": "6", "af_revenue": "6", "af_currency": "USD", "af_content_type": "1m", "af_subscription_id": "af_subscription_id", "af_quantity": "1", "af_description": "af_description"}, "eventCurrency": "USD", "ip": "1.2.3.4", "language": "\u4e2d\u6587", "lang_code": "en-us", "platform": "ios", "customer_user_id": "uid", "af_param_1": "0"}'
    for i in range(0,10000):
        #msg='msg is '+str(i)+':'+mesg
        print('i~~~' + str(i))
        producer.send(topicName,mesg.encode('utf-8'))
        sleep(3)
if __name__=='__main__':
    start_producer()
