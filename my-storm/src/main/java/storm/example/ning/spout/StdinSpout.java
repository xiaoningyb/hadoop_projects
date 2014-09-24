package storm.example.ning.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class StdinSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void nextTuple() {
	try {
          Time.sleep(100);
    } catch(InterruptedException e) {
        throw new RuntimeException(e);
    }
	File file = new File("./test/data.dat");
	BufferedReader reader = null;
	 try {
		 reader = new BufferedReader(new FileReader(file));
		 String tempString = null;
		 while ((tempString = reader.readLine()) != null) {
			 _collector.emit(new Values(tempString));
		 }
		 reader.close();
	 } catch (IOException e) {
         e.printStackTrace();
     } finally {
         if (reader != null) {
             try {
                 reader.close();
             } catch (IOException e1) {
             }
         }
     }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}