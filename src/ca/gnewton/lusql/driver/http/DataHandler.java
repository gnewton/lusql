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

public class DataHandler extends RegisterHandler
{
	public static final String STATUS_KEY="LUSQL_STATUS";
	public static final String STATUS_DATA="LUSQL_STATUS_DATA";
	public static final String STATUS_NO_DATA="LUSQL_STATUS_NO_DATA";
	public static final String STATUS_END_DATA="LUSQL_STATUS_END_DATA";
	
	private BlockingQueue<Doc[]> queue = null;	

	public DataHandler(BlockingQueue<Doc[]> queue) {
		this.queue = queue;
	}

	private long totalCount = 0l;
	
	private boolean isLast = false;
	public void handle(HttpExchange exchange) throws IOException {
		System.out.println("Start request");
		try{
			String remoteHost = exchange.getRemoteAddress().getHostName();
			System.out.println("DataHandler: " + remoteHost);
			if(!clients.contains(remoteHost)){
				exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0);
				System.out.println("Remote host not in clients: " + remoteHost);
			}
			else{
				System.out.println("Remote host ok: " + remoteHost);
				
				StringBuilder sb = new StringBuilder(1000);

				long count = 0l;
				Doc[] docs = null;	
				
				boolean first = true;
				
				while(sb.length() < 400000 && !isLast){
					try{
						docs = queue.poll(100l, TimeUnit.MILLISECONDS);
					}
					catch(Exception e){
						e.printStackTrace();
					}
					if(docs != null){
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
				totalCount += count;

				if(docs != null){
					sb.append("]");
					System.out.println("Sending " + sb.length());
					System.out.println("Sending #docs:" + count);
					System.out.println("Total #docs:" + totalCount);
					
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


