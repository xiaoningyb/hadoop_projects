package storm.example.ning;

import storm.example.ning.spout.ExclaimBolt;
import storm.example.ning.spout.SenqueceBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;

public class BasicDRPCTopology {
	
	public static void main(String[] args) throws Exception {
	    LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
	    //LinearDRPCTopologyBuilder expects the last bolt to emit an output stream containing 2-tuples of the form [id, result]. 
	    //Finally, all intermediate tuples must contain the request id as the first field.
	    builder.addBolt(new ExclaimBolt(), 3);

	    Config conf = new Config();

	    if (args == null || args.length == 0) {
	      LocalDRPC drpc = new LocalDRPC();
	      LocalCluster cluster = new LocalCluster();

	      cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));

	      for (String word : new String[]{ "hello", "goodbye" }) {
	        System.out.println("Result for \"" + word + "\": " + drpc.execute("exclamation", word));
	      }

	      cluster.shutdown();
	      drpc.shutdown();
	    }
	    else {
	      conf.setNumWorkers(3);
	      StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
	    }
	  }

}
