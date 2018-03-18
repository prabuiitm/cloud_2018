package com.deneebo.paas.storm.bolt;

import java.util.Map;


import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.deneebo.paas.storm.common.EventDao;
import com.esotericsoftware.minlog.Log;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * This class is used to emit data from spout,it will convert into jsonobject format,authenticate device ,identifying events
 * and emit new stream to rulebolt 
 * @author  deneebo
 * @version 1.0, 3/12/13
 * @since   1.0
 */
 public class CassandraWriteBolt extends BaseRichBolt   {
	private static final long serialVersionUID = 1L;
	private String deviceid;;
    OutputCollector _collector;
    public String keyspace="";
    public String keyspaceName="";
    public String devicetype="";
    public String key="";
    public JSONObject jsonstream=null;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    static int count=0;
    @Override
    public void execute(Tuple tuple) 
    { 
    	if(tuple.size()!=0)
    	{
    		count=count+1;
    		System.out.println("*******************************************");
    		System.out.println("IN cassandra Bolt Count::"+count);
    		System.out.println("*******************************************");
    		System.out.println("Tuple inside of CassandraWrite Bolt   -:"+tuple.getValue(0).toString());
    		String[] data;
    		String st=tuple.getValues().toString();
    		String stream=st.substring(1, st.length()-1);
    		//System.out.println("Stream arrived CassandraWrite  :   "+st.substring(1, st.length()-1));
    		data=stream.split(":",3);
    		keyspaceName = data[0];
    		String eventId = data[1];
    		String KEY = data[2];
    		JSONParser parser = new JSONParser();

            Object obj=new Object();
			try {
				obj = parser.parse(data[2]);
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

            JSONObject JSstream = (JSONObject) obj;
    		deviceid=(String) JSstream.get("KEY");
    		String Key[]=deviceid.split("-");         
    		if(Key.length==3)
    		{
    			deviceid=Key[2];
    			try {
    				EventDao.insertCassandra(keyspaceName,eventId,JSstream);
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}
    		else
    		{
    			try {
    				EventDao.insertformdata(keyspaceName,eventId,JSstream);
    			} catch (Exception e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}//comment this line if you don't want insert data into cassandra
    		}
    			
    		
    		
    	}
      	
        
      		
    }
    /**
     * Declare the output field "Stream"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	 declarer.declare(new Fields("Stream"));
      
    }
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}