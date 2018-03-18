package com.deneebo.paas.storm.bolt;

import java.util.Map;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.deneebo.paas.storm.common.Authentication;
import com.deneebo.paas.storm.common.EventIdentification;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * This class is used to emit data from JSONCONVERTER BOLT,it will check valid key ,authenticate device type,authenticate device,identifying events
 * and emit new stream to rulebolt 
 * @author  deneebo
 * @version 1.0, 3/12/13
 * @since   1.0
 */
public class AuthenticationBolt extends BaseRichBolt  
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/*
	 * Logger object for the AuthenticationBolt class
	 */
	static Logger logger = Logger.getLogger("AUTHEN_APPENDER");
	
	OutputCollector _OutputCollector;
	public JSONObject jsonobject=new JSONObject();
	String clientid=null;
	String devicetypeid=null;
	String deviceid=null;
	String keyspacename=null;
	String eventid=null;
	static int nonauthenticateclinetid=0;
	static int nonauthenticatedeviceid=0;
	static int nonauthenticatecount=0; //count for non authenticated
	static int nonauthenticatedevicetype=0;
	boolean uniqueidentifier_flag=false;
	boolean clientid_flag=false;
	boolean devicetypeid_flag=false;
	boolean deviceid_flag=false;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) 
	{
		_OutputCollector=collector;
		
	}
 static int count=0;
	public void execute(Tuple input) 
	{
		if(input.size()>0)
		{
			count=count+1;
			System.out.println("*******************************************");
			System.out.println("IN Authentication Bolt Count::"+count);
			System.out.println("*******************************************");
			String data=input.getValue(0).toString();
			System.out.println("Tuple inside of Authentication Bolt   -:"+data);
			 JSONObject jsonObj;
		        JSONParser parser = new JSONParser();

		        Object obj=new Object();
				try {
					obj = parser.parse(data);
				} catch (ParseException e1) {
					e1.printStackTrace();
				}
		        jsonobject = (JSONObject) obj;
			
			try{
			String[] uniqueidentifier=jsonobject.get("KEY").toString().split("-");
			if(uniqueidentifier.length==3)
			{
				clientid=uniqueidentifier[0];
				devicetypeid=uniqueidentifier[1];
				deviceid=uniqueidentifier[2];
				uniqueidentifier_flag=true;
			}
			else
			{
				logger.error("Unique Identifier Key Not Well formed"+jsonobject);
				System.out.println("Exception @ AuthenticationBolt :Unique Identifier Key Not Well formed");
			}
			}catch(Throwable e)
			{
				logger.error("Unique Identifier Key Not Found"+jsonobject);
				System.out.println("Exception @ AuthenticationBolt");
				e.printStackTrace();
			}
			
		}
		if(uniqueidentifier_flag)
		{
			nonauthenticateclinetid++;
			clientid_flag=Authentication.isautheroziedClient(clientid);
			System.out.println("******************************************************************");
			System.out.println("Non authenticatn clientid  :::"+nonauthenticateclinetid);
			System.out.println("IsClientID Authentication   -:"+clientid_flag);
			System.out.println("******************************************************************");
			if(clientid_flag)
			{
				keyspacename=Authentication.getclientKeyspace(clientid);
				devicetypeid_flag=Authentication.isautheroziedDevicetype(keyspacename,devicetypeid);
				System.out.println("IsDeviceType Authentication    -:"+devicetypeid_flag);
					/** devicetype id not exits**/
					if(!devicetypeid_flag)
					{
						
					 nonauthenticatedevicetype++;
					  String input_string=keyspacename+":"+eventid+":"+jsonobject.toString();
				     /** Need to emit value to authentication error bolt for sending alert via Email**/
					  System.out.println("****************************************************************************************************");
					  System.out.println("Non Authenticate devicetype  :: "+nonauthenticatedevicetype);
					  System.out.println("device type id wrong !.Need to intimate client......via EMAIL........clientid  :"+clientid );
					  System.out.println("****************************************************************************************************");
					  logger.error("DeviceType Authentication Fail! For the Client .Need to intimate client......via EMAIL..:clientid  :"+clientid   +":"+jsonobject);
					}
						
					deviceid_flag=Authentication.isautheroziedDevice(keyspacename,devicetypeid,deviceid);
					System.out.println("IsDeviceId authentication  -: "+deviceid_flag);
					/** deviceid not exits**/
					if(!deviceid_flag)
					{
					  nonauthenticatedeviceid++;					  
					  String deviceid_string=keyspacename+":"+eventid+":"+jsonobject.toString();
				     /** emit value to authentication error bolt**/
					  System.out.println("****************************************************************************************************");
					 System.out.println("Non Authenticate Device id "+nonauthenticatedeviceid);
					  System.out.println("****device id wrong !.Need to intimate client......via EMAIL...clientid  :"+clientid);
					  System.out.println("****************************************************************************************************");
					  logger.error("device id not registered with us !.Need to intimate client......via EMAIL   :clientid  :"+clientid   +":"+jsonobject);
					} 
				
					/** check event id when registered client,registered devicetype,registered deviceid is true**/
					if(clientid_flag && devicetypeid_flag && deviceid_flag)
					{
						eventid=EventIdentification.getEvent(keyspacename,devicetypeid,jsonobject);
						
						if(eventid!=null)
						{
							/**
							 *  Froming input to the RuleBolt
							 */
							System.out.println("Occured Event is.......-: "+eventid);
							String input_string=keyspacename+":"+eventid+":"+deviceid+":"+jsonobject.toString();
							_OutputCollector.emit(new Values(input_string));
							_OutputCollector.ack(input);
						}
						else
						{
							nonauthenticatecount++;
							System.out.println("****************************************************************************************************");
							System.out.println("Non Authenticated Event Stream ::  "+nonauthenticatecount);
							logger.error("Unmatched Event Need to intimate client..via EMAIL:   clientid  :"+clientid +":devicetypeid  -:"+devicetypeid+"  :"+jsonobject.toString());
							System.out.println("****************************************************************************************************");
							System.out.println("does not match with your event data !.Need to intimate client......via EMAIL........clientid  :"+clientid +":devicetypeid  -:"+devicetypeid);
							
						}
					}
					}
			else
			{
				 logger.error("UnAuthorized Client Communication! Need to Alert deneebo support team..via EMAIL*:"+jsonobject);
				  System.out.println("****Client ID!. Mismatched ...Alert to deneebo support team..via EMAIL****");
			}
			}
		else
		{
			 
		}
		
		}
		
/** defining output field **/
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
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
