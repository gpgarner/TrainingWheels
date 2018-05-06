/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic data

/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 8 --topic data

python kafka/kafka_producer_fec2.py
