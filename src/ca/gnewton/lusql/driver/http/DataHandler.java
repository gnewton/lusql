package ca.gnewton.lusql.driver.http;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.Compressor;
import ca.gnewton.lusql.driver.json.Doc2Json;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.util.Queue;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DataHandler extends RegisterHandler
{
	public static final String STATUS_KEY="LUSQL_STATUS";
	public static final String STATUS_DATA="LUSQL_STATUS_DATA";
	public static final String STATUS_NO_DATA="LUSQL_STATUS_NO_DATA";
	public static final String STATUS_END_DATA="LUSQL_STATUS_END_DATA";
	
	int sizeLimit = 2000000;
		
	private volatile BlockingQueue<Doc[]> queue = null;	
	private volatile AtomicLong totalCount = new AtomicLong(0);
	private volatile boolean isLast = false;


	public DataHandler(BlockingQueue<Doc[]> queue) {
		this.queue = queue;
	}

	public void handle(HttpExchange exchange) throws IOException {
		String clientId = exchange.getRequestHeaders().get(Foo.CLIENT_ID_KEY).get(0);
		System.out.println("Start request");
		try{
			String remoteHost = exchange.getRemoteAddress().getHostName();
			String completeClientKey = makeCompleteClientKey(remoteHost, clientId);
			System.out.println("DataHandler: " + completeClientKey);


			if(!clients.contains(completeClientKey)){
				exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0);
				System.out.println("Remote host id not in clients: " + completeClientKey);
			}
			else{
				System.out.println("Remote host id ok: " + completeClientKey);
				
				StringBuilder sb = new StringBuilder(1000);

				long count = 0l;
				Doc[] docs = null;	
				
				boolean first = true;

				int numDocLists = 0;
				
				while(sb.length() < sizeLimit && !isLast){
					try{
						docs = queue.poll(100l, TimeUnit.MILLISECONDS);
					}
					catch(Exception e){
						e.printStackTrace();
					}
					if(docs != null){
						++numDocLists;
						
						if(first){
							sb.append("[\n");
							first = false;
						}
						count += (long)(docs.length);
						for(int j=0; j<docs.length; j++){

							if(docs[j].isLast()){
								isLast = true;
								System.out.println("ISLAST " + j);
								break;
							}else{
								if(j != 0){
									sb.append(",\n");
								}
								sb.append(Doc2Json.doc2json(docs[j]));
							}
						}
					}
				}
				
				totalCount.incrementAndGet();

				if(docs != null){
					sb.append("]");

					System.out.println("Sending " + sb.length());
					System.out.println("Sending #docs: " + count);
					System.out.println("Total #docs: " + totalCount);
					System.out.println("Total #docsLists: " + numDocLists);
					
					//System.out.println(sb);
					byte[] response = Compressor.compress((sb.toString()).getBytes("UTF-8"));
					System.out.println("response length=" + response.length);
					
					Headers headers = exchange.getResponseHeaders();
					
					if(isLast){
						headers.put(STATUS_KEY, makeList(STATUS_END_DATA));
						System.out.println("SENDING ISLAST");
					}
					else{
						headers.put(STATUS_KEY, makeList(STATUS_DATA));
					}
					exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK,
					                             response.length);
					exchange.getResponseBody().write(response);
				}else
					{
						Headers headers = exchange.getResponseHeaders();
						headers.put(STATUS_KEY, makeList(STATUS_NO_DATA));
						exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
					}
				
				System.out.println("Response body sent");
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		finally{
			exchange.close();
			System.out.println("End request");
		}
	}
	

	private List<String> makeList(String s)
	{
		List<String>list = new ArrayList<String>();
		list.add(s);
		return list;
	}
	
}


