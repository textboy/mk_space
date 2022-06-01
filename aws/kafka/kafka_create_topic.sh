## create topic for AWS kafka
# https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html
sudo yum install java-1.8.0
cd /home/ec2-user/so
wget https://archive.apache.org/dist/kafka/2.6.2/kafka_2.12-2.6.2.tgz
tar -xzf kafka_2.12-2.6.2.tgz -C /home/ec2-user/pgm/
cd /home/ec2-user/pgm/kafka_2.12-2.6.2
bin/kafka-topics.sh --create --zookeeper b-2.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092,b-1.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092,b-3.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092 --replication-factor 3 --partitions 1 --topic mk_topic_1
bin/kafka-topics.sh --bootstrap-server b-1.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092 --list
bin/kafka-topics.sh --bootstrap-server b-1.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092 --create --topic mk-topic-1 --replication-factor 2
