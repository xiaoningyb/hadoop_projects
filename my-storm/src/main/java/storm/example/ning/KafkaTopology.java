package storm.example.ning;

import java.util.HashMap;
import java.util.Map;

import storm.example.ning.scheme.MessageScheme;
import storm.example.ning.spout.SenqueceBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class KafkaTopology {
	public static void main(String[] args) throws Exception {

		// 配置Zookeeper地址
	    BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
	    // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
	    SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "test", "/zkkafkaspout" , "kafkaspout");
	     
	    // 配置KafkaBolt中的kafka.broker.properties
	    Config conf = new Config();  
		Map<String, String> map = new HashMap<String, String>(); 
		// 配置Kafka broker地址       
	    map.put("metadata.broker.list", "localhost:9092");
		// serializer.class为消息的序列化类
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
		// 配置KafkaBolt生成的topic
		conf.put("topic", "topic_out");
	      
	    spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		  
	    TopologyBuilder builder = new TopologyBuilder();
	    builder.setSpout("spout", new KafkaSpout(spoutConfig)); 
	    builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout"); 
	    builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt"); 
	    
	    conf.setDebug(true);

	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(5);

	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	    else {
	      conf.setMaxTaskParallelism(5);

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("kafka_topo", conf, builder.createTopology());
	      Thread.sleep(100000);
	      cluster.killTopology("kafka_topo"); 
	      cluster.shutdown();
	    }
	  }
}
