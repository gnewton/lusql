package ca.gnewton.lusql.driver.http;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.net.*;


public class HttpDocSource extends AbstractDocSource
//implements DocSource
{
	public static final String SourceUrlKey="sourceUrl";

	private String baseUrl="http://localhost:8888";
	//private String baseUrl="http://192.168.0.101:8888";

	private final String registerUrl = baseUrl + HttpDocSink.PATH_REGISTER;
	private final String unregisterUrl = baseUrl + HttpDocSink.PATH_UNREGISTER;
	private final String dataUrl = baseUrl + HttpDocSink.PATH_DATA;

	private volatile boolean first = true;
	private volatile BlockingQueue<Doc> queue;
	private volatile boolean successfulRegister = false;

	private Foo foo = null;
	
	
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
			e.printStackTrace();
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
	

	public String showState(int level)
	{
		return null;	
	}
	
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
	
	private void extractProperties(MultiValueProp p)
	{
		if(p.containsKey(SourceUrlKey)){
			setBaseUrl(p.get(SourceUrlKey).get(0));
		}
	}



	public void setSuccessfulRegister(final boolean successfulRegister)
	{
		this.successfulRegister	= successfulRegister;
	}

	public void setBaseUrl(final String baseUrl)
	{
		this.baseUrl = baseUrl;
	}
	
	
}
