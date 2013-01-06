package ca.gnewton.lusql.driver.http;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.net.*;


public class HttpDocSource 
	implements DocSource
{
	public static final String SourceUrlKey="sourceUrl";

	//private String baseUrl="http://localhost:8888";
	private String baseUrl="http://192.168.0.101:8888";

	private String registerUrl = baseUrl + HttpDocSink.PATH_REGISTER;
	private String unregisterUrl = baseUrl + HttpDocSink.PATH_UNREGISTER;
	private String dataUrl = baseUrl + HttpDocSink.PATH_DATA;

	private Foo foo = null;
	
	BlockingQueue<Doc> queue;
	
	public void init(MultiValueProp p) throws PluginException
	{
		extractProperties(p);
		queue = new ArrayBlockingQueue(10);
		foo = new Foo();
		foo.init(queue, registerUrl, unregisterUrl, dataUrl, this);
		foo.start();

		try{
			Thread.currentThread().sleep(3000);		
		}
		catch(Exception e){
			
		}

	}
	

	public Properties explainProperties()
	{
		return null;
	}
	
	public String description()
	{
		return null;
	}
	
	public void done() throws PluginException
	{
		
	}
	

	public String showState(int level) throws PluginException
	{
		return null;	
	}
	
	public boolean isThreaded()
	{
		return false;
	}
	
	public void setThreaded(boolean b)
	{
		
	}
	

	public boolean isThreadSafe()
	{
		return true;
	}
	
	public void setThreadSafe(boolean b)
	{
		
	}
	
	boolean first = true;
	
	public Doc next()  throws DataSourceException
	{
		try{
			if(first){
				Thread.currentThread().sleep(500);
				first = false;
			}
			
			int pollCount = 0;
			while(pollCount < 30){
				if(!successfulRegister){
					throw new DataSourceException("Failed to register with source url");
				}
				
				Doc doc = queue.poll(2000l, TimeUnit.MILLISECONDS);
				if(doc != null)
					return doc;
				try{
					System.out.println("queue poll");
					Thread.currentThread().sleep(300);
					++pollCount;
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
			throw new DataSourceException();
		}
		throw new DataSourceException("Unable to get data from url");
	}
	
	public void addField(String field)
	{
		
	}
	
	public boolean supportsCompression()
	{
		return false;
	}
	

	public boolean isSupportsReadingFromStdin()
	{
		return false;
	}
	
	public void setReadingFromStdin(boolean b)
	{
		
	}
	
	public boolean getReadingFromStdin()
	{
		return false;
	}



	///private
	private void extractProperties(MultiValueProp p)
	{
		
	}

	boolean successfulRegister = false;

	public void setSuccessfulRegister(final boolean successfulRegister)
	{
		this.successfulRegister	= successfulRegister;
	}

	public void setBaseUrl(final String baseUrl)
	{
		this.baseUrl = baseUrl;
	}
	
	
}
