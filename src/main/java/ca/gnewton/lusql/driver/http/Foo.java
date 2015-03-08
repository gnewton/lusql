package ca.gnewton.lusql.driver.http;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import ca.gnewton.lusql.driver.json.Doc2Json;
import java.util.*;
import java.io.*;
import org.json.simple.*;
import org.json.simple.parser.*;
import java.util.concurrent.BlockingQueue;
import java.lang.Math;


import java.net.*;

public class Foo extends Thread
{
	public static final String CLIENT_ID_KEY="clientIdKey";
	private BlockingQueue<Doc> queue;

	private String registerUrl = null;
	private String unregisterUrl = null;
	private String dataUrl = null;
	private int maxDocListSize = 0;

	private HttpDocSource hds = null;

	private String clientKey = null;
	private JSONParser parser=null;
	
	public void init(final BlockingQueue<Doc> queue, final String registerUrl, final String unregisterUrl, final String dataUrl,
	                 final HttpDocSource hds)
	{
		parser=new JSONParser();
		this.queue = queue;
		this.registerUrl = registerUrl;
		this.unregisterUrl = unregisterUrl;
		this.dataUrl = dataUrl;
		this.hds = hds;

		Random r = new Random();
		clientKey =  Long.toString(System.nanoTime()) 
			+ "_" 
			+ Long.toString(Math.abs(r.nextLong()));
		try{
			System.out.println("Foo:: registering " + registerUrl);
			register(registerUrl);
			System.out.println("Foo:: registered " + registerUrl);
		}
		catch(Exception e){
			e.printStackTrace();
			hds.setSuccessfulRegister(false);
			return;
		}
		hds.setSuccessfulRegister(true);
	}


	public void run()
	{
		try{
			while(true){
				if(queue.size() <= maxDocListSize/4){
					try{
						if(!populateQueue()){
							Doc doc = new DocImp();
							doc.setLast(true);
							queue.put(doc);
							break;
						}
						
					}
					catch(Exception e)
						{
							e.printStackTrace();
							Doc doc = new DocImp();
							doc.setLast(true);
							try{
								queue.put(doc);							
							}
							catch(Exception e2){
								e2.printStackTrace();
							}
							finally{
								break;								
							}
						}
				}
				try{
					Thread.currentThread().sleep(100);
				}
				catch(Exception e){
					e.printStackTrace();
					Doc doc = new DocImp();
					doc.setLast(true);
					try{
						queue.put(doc);							
					}
					catch(Exception e3){
						e3.printStackTrace();
					}
					finally{
						break;								
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try{
				unregister(unregisterUrl);
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
		}
	}



	private boolean populateQueue()
		throws DataSourceException
	{
		String s = getUrlContent(dataUrl);
		if(s == null)
			return false;
		
		Object obj=null;
		try{
			obj=parser.parse(s);
		}
		catch(Exception e){
			e.printStackTrace();
			throw new DataSourceException("Problem parsing json");
		}
		
		//Object obj=JSONValue.parse(s);
		JSONArray array=(JSONArray)obj;
		if(array.size() > maxDocListSize){
			maxDocListSize = array.size();
		}
		
		
		for(int i=0; i<array.size(); i++){
			JSONObject d=(JSONObject)array.get(i);
			try{
				queue.put(Doc2Json.json2doc(d));
			}
			catch(Exception e){
				e.printStackTrace();
				throw new DataSourceException("Problem putting to queue");
			}
			
			
		}
		return true;
	}

	private String getUrlContent(String urlString) 
		throws DataSourceException
	{
		HttpURLConnection conn=null;
		BufferedReader rd=null;
		String line;
		String content;
		
		URL url;		
		try {			
			url = new URL(urlString);
			conn = (HttpURLConnection) url.openConnection();
			conn.addRequestProperty(Foo.CLIENT_ID_KEY,  clientKey);
			System.out.println("Sending header: " + Foo.CLIENT_ID_KEY + "=" +  clientKey);
			
			conn.setRequestMethod("GET");
			int responseCode = conn.getResponseCode();

			String status = conn.getHeaderField(DataHandler.STATUS_KEY);
			if(status != null && status.equals(DataHandler.STATUS_NO_DATA))
				return null;

			
			if(responseCode != HttpURLConnection.HTTP_OK)
				throw new DataSourceException("Failed http connection: HTTP return code=" + responseCode);
			
			byte[] buf = Compressor.decompress(conn.getInputStream());
			content = new String(buf, "UTF-8");
		}
		catch(DataSourceException e){
			throw e;
		}
		catch(Exception e){
			e.printStackTrace();
			throw new DataSourceException("Unable to read from url " + urlString);
		}
		return content.toString();
	}

	private void emptyGet(String urlString, String mes)
		throws DataSourceException
	{
		URL url;		
		try {			
			url = new URL(urlString);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.addRequestProperty(Foo.CLIENT_ID_KEY,  clientKey);
			conn.setRequestMethod("GET");
			int responseCode = conn.getResponseCode();
			if(responseCode != HttpURLConnection.HTTP_OK)
				throw new DataSourceException("Failed http connection: HTTP return code=" + responseCode);
			System.out.println(mes + ": OK");
		}
		catch(DataSourceException e){
			throw e;
		}
		catch(Exception e){
			e.printStackTrace();
			throw new DataSourceException("Unable to " + mes + " " + urlString);
		}
	}
	

	private void register(String urlString)
		throws DataSourceException
	{
		emptyGet(urlString, "Register");
	}

	private void unregister(String urlString)
		throws DataSourceException
	{
		emptyGet(urlString, "Unregister");
	}
	
	private static final String list2String(List<String> list)
	{
		StringBuilder sb = new StringBuilder();
		for(String s: list){
			sb.append("[" + s + "] ");
		}
		return sb.toString();
	}
	

}
