/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic datatwo

/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 9 --topic datatwo

python kafka/kafka_producer_fec2.py
