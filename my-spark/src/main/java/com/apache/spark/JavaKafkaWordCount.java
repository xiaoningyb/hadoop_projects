package com.apache.spark;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 *   `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
 *    zoo03 my-consumer-group topic1,topic2 1`
 */

public final class JavaKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");
  private static final AtomicLong wordCount = new AtomicLong(0);

  private JavaKafkaWordCount() {
  }
  
  /*
  private static Function2<List<Long>, Optional<Long>, Optional<Long>>
  	COMPUTE_RUNNING_SUM = (nums, current) -> {
  		long sum = current.or(0L);
  		for (long i : nums) {
  			sum += i;
    }
    return Optional.of(sum);
  };
  */

  public static void main(String[] args) {
    if (args.length < 4) {
      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }
    
    System.out.println("JavaKafkaWordCount : zkQuorum:" + args[0] + ", group:" + args[1] 
    					+ ", topics:" + args[2] + ", numThreads:" + args[3]);

    //StreamingExamples.setStreamingLogLevels();
    //==================================Init stream=============================================
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Create the context with a 1 second batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(10000));
    jssc.checkpoint("file:///tmp/spark-streaming");

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[2].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }

    JavaPairReceiverInputDStream<String, String> messages =
            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
    //==========================================================================================

    //==================================transform===============================================
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
    	System.out.println("messages.map : tuple2._1:" + tuple2._1 + ", tuple2._2:" + tuple2._2 + ", string:" + tuple2.toString() );
        return tuple2._2();
      }
    });

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
    	System.out.println("======lines.flatMap : " + x);
        return Lists.newArrayList(SPACE.split(x));
      }
    });

    JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
    	@Override
    	public Tuple2<String, Integer> call(String s) {
    		System.out.println("======mapToPair : "+ s);	
    		return new Tuple2<String, Integer>(s, 1);
    		}
    	});

    JavaPairDStream<String, Integer> wordCounts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          System.out.println("======reduceByKey : i1=" + i1 + ", i2="+ i2); 
          return i1 + i2;
        }
      });
    
    /*
    JavaPairDStream<String, Integer> runningCounts = wordCounts.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
        
    		@Override 
    		public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
    			Integer sum = 0;
    			for(Integer value : values) {
    				sum += value;
    			}
    			Integer newSum = state.get() + sum;
    			System.out.println("======newSum :" + newSum); 
    			return Optional.of(newSum);
    		}
    });
    */
    //==========================================================================================
    
    //====================================windows operation=====================================
    JavaPairDStream<String, Integer> windowCounts = wordCounts.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          System.out.println("======reduceByKeyAndWindow : i1=" + i1 + ", i2="+ i2); 
          return i1 + i2;
        }
      }, new Duration(10000), new Duration(10000));
    
    
    JavaPairDStream<String, Integer> windowHCounts = windowCounts.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          System.out.println("======reduceByKeyAndWindowMin : i1=" + i1 + ", i2="+ i2); 
          return i1 + i2;
        }
      }, new Duration(60000), new Duration(60000));
    //==========================================================================================
    
    //====================================updateStateByKey=====================================
    /*
    windowCounts.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
    	@Override
        public Void call(JavaPairRDD<String, Integer> rdd) {
    		List<Tuple2<String, Integer>> ret = rdd.collect();
    		System.out.println("======rdd.collect() size = " + ret.size());
    		for (Tuple2<String, Integer> r : ret) {
    			System.out.println("======wordCount add " + r._2() +" for input " + r._1());
    			wordCount.addAndGet(r._2());
    		}
    		System.out.println("======wordCount =  " + wordCount.get());
    		return null;
    	}
      });
    */
    
    JavaPairDStream<String, Integer> updated = windowCounts.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
    	
    	@Override
        public Optional<Integer> call(List<Integer> Integers, Optional<Integer> current) {
    		System.out.println("======current :" + current.or(0));
    		Integer sum = current.or(0);
    		for (Integer i : Integers) {
    			System.out.println("======sum add " + i);
    			sum += i;
    		}
    		System.out.println("======sum =  " + sum);
    		return Optional.of(sum);
    	}
    });
    
    
    //====================================updateStateByKey=====================================
    //==========================================================================================
    
    
    //ones.print();
    //wordCounts.	print();
    //windowCounts.print();
    //runningCounts.print();
    //windowHCounts.print();
    updated.print();
    jssc.start();
    jssc.awaitTermination();
  }
}
