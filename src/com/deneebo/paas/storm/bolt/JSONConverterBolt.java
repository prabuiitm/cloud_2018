package com.deneebo.paas.storm.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;


import com.deneebo.paas.storm.common.JsonConverter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * This class is used to emit data from spout,it will convert into jsonobject and emit to AuthenticationBolt
 * @author  deneebo
 * @version 1.0, 3/12/13
 * @since   1.0
 */
public class JSONConverterBolt extends BaseRichBolt   {
	 static Logger logger = Logger.getLogger("JSONCONVERTER_APPENDER");
	 public JSONObject jsonstream=null;
	 OutputCollector _collector;
  @Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;

	}
  static int count=0;
	@Override
	public void execute(Tuple input)
	{
		count=count+1;
		System.out.println("*******************************************");
		System.out.println("IN JsonConverter Bolt Count::"+count);
		System.out.println("*******************************************");
		System.out.println("Tuple inside of JsonConverter Bolt   -:"+input.getString(0));
		jsonstream=JsonConverter.convert(input.getString(0));//Convert Any string into Json format
		System.out.println("Tuple After converted into JSON      -:"+jsonstream.toString());
		if(input.size()!=0)
		{
			try
			{
			_collector.emit(new Values(jsonstream));
			_collector.ack(input);
			}
			catch(Exception e)
			{
			logger.error("Error parsing message to AuthenticationBolt",e);
			e.printStackTrace();
			}
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
		 declarer.declare(new Fields("Stream"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
