package com.deneebo.paas.storm.topology;

import com.deneebo.paas.storm.kafka.KafkaSpout;

import org.apache.log4j.PropertyConfigurator;
import com.deneebo.paas.storm.bolt.AuthenticationBolt;
import com.deneebo.paas.storm.bolt.CassandraWriteBolt;
import com.deneebo.paas.storm.bolt.JSONConverterBolt;
import com.deneebo.paas.storm.bolt.RuleBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
/**
 * This class is used for submit storm topology into  Local/cluster Mode
 * @author deneebo
 * @version 1.0, 02/12/13
 * @since   1.0
 */

public class DeneeboTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException 
	{
	    System.out.println("Topology Starting........");
	    /**
		* PropertiesConfigurator is used to configure logger from properties file
		*/
		PropertyConfigurator.configure("deneebolog4j/log4j.properties");
		/**
		 * Topology definition
		 */
		TopologyBuilder builder = new TopologyBuilder();		
	   	builder.setSpout("kafka", new KafkaSpout());
	   	builder.setBolt("JsonConverterBolt", new JSONConverterBolt(),5).setNumTasks(128).shuffleGrouping("kafka"); 					
	   	builder.setBolt("AuthenticationBolt", new AuthenticationBolt(),5).setNumTasks(128).shuffleGrouping("JsonConverterBolt");
	   	builder.setBolt("ThresholdBolt", new RuleBolt(),5).setNumTasks(128).shuffleGrouping("AuthenticationBolt");
		builder.setBolt("cassandraBolt", new CassandraWriteBolt(),5).setNumTasks(128).shuffleGrouping("ThresholdBolt");
		/**
		 *  create a storm Config object
		 */
		Config config = new Config();
		/**
		 *  configure kafka spout (values are available as constants on ConfigUtils)
		 */
		config.put("kafka.spout.topic", "deneebo");
		/**
		 *  kafka consumer configuration, see below
		 * 
		 */
		//config.put("kafka.zookeeper.connect", "192.168.0.243:2182,192.168.0.242:2182,192.168.0.241:2182");
		config.put("kafka.zookeeper.connect", "127.0.0.1:2181");
		config.put("kafka.consumer.timeout.ms", 1000);
		/**
		 * submit topology into cluster
		 */
		if(args!=null && args.length > 0)
        {
			config.setNumWorkers(5);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            System.out.println("Topology submitted into Storm Cluster ........");
        }
		/**
		 * submit topology into local 
		 */
    else{
    	   LocalCluster cluster = new LocalCluster();
    	   cluster.submitTopology("deneebo-storm-local", config, builder.createTopology());
           System.out.println("Topology submitted into Local Mode........");
           Utils.sleep(10000);
           
        }
    
	}
}
