package ca.gnewton.lusql.driver.http;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.util.*;
import java.io.*;

import com.sun.net.httpserver.*;
import java.net.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class HttpDocSink 
	extends AbstractDocSink
	        //implements DocSink
{
	public static final String SinkPortKey="port";
	public static final String PATH_REGISTER="/register";
	public static final String PATH_UNREGISTER="/unregister";
	public static final String PATH_DATA="/data";

	private volatile long count = 0l;
	private volatile BlockingQueue<Doc[]> queue = null;

	private int port=8888;
	public long waitTimeMillisForClientUnregister = 30l * 1000l;
	
	public HttpDocSink() {

	}

	public void commit() throws DocSinkException
	{
		
	}

	private volatile HttpServer httpServer = null;
	private volatile Set<String>clients = null;
	
	public void init(MultiValueProp p) 
		throws PluginException
	{
		extractProperties(p);
		setSupportsWritingToStdout(false);
		setSupportsCompression(false);
		setThreadSafe(true);
		
		try{
			queue = new ArrayBlockingQueue<Doc[]>(20);
			
			InetSocketAddress address = new InetSocketAddress(port);
			httpServer = HttpServer.create(address, 0);
			
			clients = new HashSet<String>();

			// create and register our handler
			HttpHandler dataHandler = new DataHandler(queue);
			((DataHandler)dataHandler).setClients(clients);
			httpServer.createContext(PATH_DATA, dataHandler);

			HttpHandler registerHandler = new RegisterHandler();
			((RegisterHandler)registerHandler).setClients(clients);
			httpServer.createContext(PATH_REGISTER, registerHandler);

			HttpHandler unregisterHandler = new UnregisterHandler();
			((RegisterHandler)unregisterHandler).setClients(clients);
			httpServer.createContext(PATH_UNREGISTER, unregisterHandler);
			httpServer.start();
		}
		catch(Throwable t){
			t.printStackTrace();
			throw new PluginException();
		}
	}
		
	
	public void addDoc(Doc[] docList)  
		throws DocSinkException
	{
		try{
			queue.put(docList);
			count += (long)(docList.length);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public void done()  
		throws PluginException
	{
		long startDoneTime = System.currentTimeMillis();
		
		while(true){
			long timeNow = System.currentTimeMillis();
			if(timeNow - startDoneTime > waitTimeMillisForClientUnregister)
				{
					System.out.println("Done: shutting down timeout expired: waiting: queue=" + queue.size()
					                   + "  clients=" + clients.size());
					break;
				}
					  
			System.out.println("Done: waiting: queue=" + queue.size()
			                   + "  clients=" + clients.size());
			if(queue.size() == 0 && clients.size() == 0){
				break;
			}
			try{
				Thread.currentThread().sleep(1000);
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		System.out.println("Total docs enqueued: " + count);
		
		System.out.println("Shutting down http server");
		
		httpServer.stop(10);
	}
	
	public String description()
	{
		return "Sink that exposes documents via http rest interface";
	}

	public Object internal()  throws DocSinkException
	{
		return null;
	}
	

	public boolean requiresPrimaryKeyField()
	{
		return false;
	}


	public boolean getWritingToStdout()
	{
		return false;
	}

	public void setWritingToStdout(boolean b)
	{
	
	}

	public final boolean isRemoveOnDone() {
		return false;
	}


	public final void setThreaded(final boolean newThreaded) {
		
	}    

	public String showState(int n)
	{
		return "";
	}

	public Properties explainProperties()
	{
		Properties p = new Properties();
		return p;
	}
	
	private void extractProperties(MultiValueProp p)
	{
		if(p.containsKey(SinkPortKey)){
			port=Integer.parseInt(p.get(SinkPortKey).get(0));
		}
	}
			
	

}
