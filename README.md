hadoop mr, yarn, spark on yarn

1.run mr
hadoop jar target/my-hadoop-1.0-SNAPSHOT.jar org.apache.hadoop.mr.App /user/ning/wordcount/in /user/ning/wordcount/out4

2.run yarn
hadoop jar target/hadoop-yarn-1.0-SNAPSHOT.jar org.apache.hadoop.mr.App /user/ning/wordcount/in /user/ning/wordcount/out4

3.run the spark yarn
spark-submit --class com.apache.spark.WordCount --master yarn-cluster --num-executors 3 --driver-memory 4g --executor-memory 2g --executor-cores 1 ./target/my-spark-1.0-SNAPSHOT.jar /user/ning/wordcount/in
