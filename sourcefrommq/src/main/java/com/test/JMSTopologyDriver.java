package com.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class JMSTopologyDriver {
	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
		TopologyBuilder tb = new TopologyBuilder();
		tb.setSpout("jmssp", new JMSSpout());
		tb.setBolt("JMSBL", new JMSBolt());
		Config conf = new Config();
		conf.setDebug(false);	

		if (args != null && args.length > 0) {

			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar("Wordcount", conf, tb.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("My JMS Topology", conf, tb.createTopology());
			Thread.sleep(60000);
			cluster.shutdown();
		}

	}

}
