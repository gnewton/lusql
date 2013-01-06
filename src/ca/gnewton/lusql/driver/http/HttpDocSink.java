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
	implements DocSink
{
	public static final String SinkURLKey="sinkUrl";
	private BlockingQueue<Doc[]> queue = null;

	int PORT=8888;
	static public String PATH_REGISTER="/register";
	static public String PATH_UNREGISTER="/unregister";
	static public String PATH_DATA="/data";
	
	public HttpDocSink() {

	}

	public void commit() throws DocSinkException
	{
		
	}

	HttpServer httpServer = null;
	Set<String>clients = null;
	
	public void init(MultiValueProp p) 
		throws PluginException
	{
		try{
			queue = new ArrayBlockingQueue<Doc[]>(20);
			
			InetSocketAddress address = new InetSocketAddress(PORT);
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
		

	private long count = 0l;
	
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
		while(true){
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
	
	public Object internal()
	{
		return null;
	}

	public final boolean isSupportsCompression() {
		return false;
	}

	public final void setPrimaryKeyField(final String newPrimaryKeyField) {
		
	}

	public boolean isSupportsWritingToStdout()
	{
		return false;
	}

	public boolean isThreadSafe()
	{
		return true;
	}

	public final boolean isThreaded() {
		return true;
	}


	public String description()
	{
		return "Sink that exposes documents via http rest interface";
	}

	public boolean requiresPrimaryKeyField()
	{
		return false;
	}

	public final void setRemoveOnDone(final boolean newRemoveOnDone) {
		
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

	public void setThreadSafe(final boolean newThreadSafe)
	{
	
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
	
			
	

}
