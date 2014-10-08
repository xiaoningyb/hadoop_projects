#hadoop mr, yarn, spark, storm on yarn

##1.run mr
<code>hadoop jar target/my-hadoop-1.0-SNAPSHOT.jar org.apache.hadoop.mr.App /user/ning/wordcount/in /user/ning/wordcount/out4</code>

##2.run yarn
<code>hadoop jar target/hadoop-yarn-1.0-SNAPSHOT.jar org.apache.hadoop.mr.App /user/ning/wordcount/in /user/ning/wordcount/out4</code>

##3.run the spark yarn
<code>spark-submit --class com.apache.spark.WordCount --master yarn-cluster --num-executors 3 --driver-memory 4g --executor-memory 2g --executor-cores 1 ./target/my-spark-1.0-SNAPSHOT.jar /user/ning/wordcount/in</code>

##4.run the strom
local: <code>storm jar ./target/my-storm-1.0-SNAPSHOT-jar-with-dependencies.jar storm.example.ning.WordCountTopology</code>

remote: <code>storm jar ./target/my-storm-1.0-SNAPSHOT-jar-with-dependencies.jar storm.example.ning.WordCountTopology my-storm</code>

##4.run the strom with kafaka
start kafka: <code>kafka-server-start.sh config/server.properties</code>

create input topic: <code>kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test</code>

create output topic: <code>kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic_out</code>

producter: <code>kafka-console-producer.sh --broker-list localhost:9092 --topic test</code>

consumer: <code>kafka-console-consumer.sh --zookeeper localhost:2181 --topic topic_out --from-beginning</code>

local: <code>storm jar ./target/my-storm-1.0-SNAPSHOT-jar-with-dependencies.jar storm.example.ning.KafkaTopology</code>

remote: <code>storm jar ./target/my-storm-1.0-SNAPSHOT-jar-with-dependencies.jar storm.example.ning.KafkaTopology kafka</code>

##5.run the spark streaming with kafaka
local <code>spark-submit --class com.apache.spark.JavaKafkaWordCount --master local[*] --num-executors 3 --driver-memory 1g --executor-memory 1g --executor-cores 1 ./target/my-spark-1.0-SNAPSHOT-jar-with-dependencies.jar localhost:2181 group spark-stream 1</code>

remote(driver on client) <code>spark-submit --class com.apache.spark.JavaKafkaWordCount --master yarn-client --num-executors 3 --driver-memory 1g --executor-memory 1g --executor-cores 1 ./target/my-spark-1.0-SNAPSHOT-jar-with-dependencies.jar localhost:2181 group spark-stream 1</code>

remote(driver on cluster) <code>spark-submit --class com.apache.spark.JavaKafkaWordCount --master yarn-cluster --num-executors 3 --driver-memory 1g --executor-memory 1g --executor-cores 1 ./target/my-spark-1.0-SNAPSHOT-jar-with-dependencies.jar localhost:2181 group spark-stream 1</code>