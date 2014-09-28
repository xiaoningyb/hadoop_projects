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
  int _count = 0;

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
	
	if(_count > 100)
	{
		System.out.println(_count+" try to emit tuple by success_stream");
	    _collector.emit("SUCCESS_STREAM", new Values(""));
	    try {
	          Time.sleep(10000);
	    } catch(InterruptedException e) {
	        throw new RuntimeException(e);
	    }
	    return;
	}
	
	File file = new File("./test/data.dat");
	BufferedReader reader = null;
	 try {
		 reader = new BufferedReader(new FileReader(file));
		 String tempString = null;
		 while ((tempString = reader.readLine()) != null) {
			 _collector.emit(new Values(tempString));
			 _count++;
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
    declarer.declareStream("SUCCESS_STREAM",new Fields("word"));
  }

}