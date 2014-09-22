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
