package com.deneebo.paas.storm.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.deneebo.paas.storm.cassandra.CassandraConnector;

/**
 * class description
 * @author 
 * @version
 */


public class RuleService 
{
	static Statement statement=null;
	static ResultSet resultset=null;
	/**
	 * 
	 * @param ruleid
	 * @param service
	 * @param triggermessage
	 * @param keyspacename
	 */
	public static void executeRule(String ruleid,String service,String triggermessage,String keyspacename)
	{
	
		String services[]=service.split(",");
		try
		{
			statement=CassandraConnector.getConnection(keyspacename);
			String query="select * from rulesaction where key='"+ruleid+"'";
			resultset=statement.executeQuery(query);
			for (int i = 0; i < services.length; i++) 
			{
				switch(services[i])
				{
					case "Email" : 	
									String emailid=resultset.getString("emailid");
									String emailmessage=resultset.getString("emailbody");
									String  subject=resultset.getString("subject");
									RuleService.Email(emailid, subject, emailmessage,triggermessage);
									/*SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
									Date date=new Date();
									String senttime = formatter.format(date);
									String key=senttime+emailid;
									query="insert into emailservice(key,emailto,emailsubject,emailbody)values('"+key+"','"+emailid+"','"+subject+"','"+emailmessage+"')";
									//statement.executeQuery(query);
									System.out.println(query);*/
									break;
					case "SMS"	:	String mobilenumber=resultset.getString("mobilenumber");
									String message=resultset.getString("message");
									RuleService.SMS(mobilenumber, message);
									break;
					default : break;
				}
			}
			
		}
		catch(Exception e)
		{
			
		}
	}
	/**
	 * 
	 * @param mobilenumber
	 * @param message
	 */
	public  static void SMS(String mobilenumber, String message) 
	{
		try
		{
			
		}
		catch(Exception e)
		{
			
		}
		
	}
	/**
	 * 
	 * @param emailid
	 * @param subject
	 * @param message
	 * @param triggermessage 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void Email(String emailto, String subject, String emailmessage, String triggermessage) throws FileNotFoundException, IOException 
	{
		
		Properties prop = new Properties();
		final String username;
		final String password;
		try {
		prop.load(new FileInputStream("deneebolog4j/MailUtils.properties"));
		username=prop.getProperty("MailSender");
		password=prop.getProperty("MailSenderPassword");
		prop.put("mail.smtp.host", "smtp.gmail.com");
		prop.put("mail.smtp.socketFactory.port", "465");
		prop.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
		prop.put("mail.smtp.auth", "true");
		prop.put("mail.smtp.port", "465");

		Session session = Session.getInstance(prop,new javax.mail.Authenticator()
		{
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username,password);
			}
		  });
		Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(username));
			message.setRecipients(Message.RecipientType.TO,
			InternetAddress.parse(emailto));
			message.setSubject(subject);
			message.setText(emailmessage);
			System.out.println("before mail send");
			message.setContent("<h4>Dear Customer,</h4><style='padding-left: 20px; padding-top:10px; width:760px; font-size:12px; color:#fff;'><p>"+emailmessage+"<br><br><u>Note:</u><br><br>"+triggermessage+"<br><br>Sincerly,<br></p><p>Deneebo support </p><p>*** This is an automatically generated email, please do not reply ***</p></style>","text/html" );
			//Transport.send(message);
			System.out.println("Email send");
		
		}
		catch (MessagingException e)
		{
			throw new RuntimeException(e);
		}
		
	
	}
}
