package storm.example.ning;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
import storm.example.ning.spout.SenqueceBolt;
import storm.example.ning.spout.StdinSpout;
import storm.example.ning.scheme.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
  public static class SplitSentence extends ShellBolt implements IRichBolt {

    public SplitSentence() {
      super("python", "splitsentence.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      System.out.println("==========="+this);
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      System.out.println(word+", count="+count);
      dumpToFile() ;
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
    
    public void dumpToFile() {
    	File file = new File("./test/result.dat");
    	BufferedWriter writer = null;
    	 try {
    		 writer = new BufferedWriter(new FileWriter(file));
    		 String tempString = null;
    		 
    		 Iterator iter = counts.entrySet().iterator();
    		 while (iter.hasNext()) {
    			Map.Entry entry = (Map.Entry) iter.next();
    			Object key = entry.getKey();
    			Object val = entry.getValue();
    			System.out.println("==="+key+", count="+val);
    			writer.write(key+",count="+val+"\n");
    		 }
    	 
    		 writer.close();
    	 } catch (IOException e) {
             e.printStackTrace();
         } finally {
             if (writer != null) {
                 try {
                	 writer.close();
                 } catch (IOException e1) {
                 }
             }
         }
    }
  }

  public static void main(String[] args) throws Exception {
	  
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new StdinSpout(), 5);
    builder.setBolt("split", new SplitSentence(), 16).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 3).fieldsGrouping("split", new Fields("word"));
    Config conf = new Config();  
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(5);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(5);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);
      
      cluster.shutdown();
    }
  }
}