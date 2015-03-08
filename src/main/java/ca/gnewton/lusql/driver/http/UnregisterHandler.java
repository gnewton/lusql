package ca.gnewton.lusql.driver.http;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.Compressor;
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class UnregisterHandler extends RegisterHandler
{

	public UnregisterHandler() {

	}

	public void handle(HttpExchange exchange) throws IOException {
		try{
			String remoteHost = exchange.getRemoteAddress().getHostName();
			String clientId = exchange.getRequestHeaders().get(Foo.CLIENT_ID_KEY).get(0);
			
			String completeClientKey = makeCompleteClientKey(remoteHost, clientId);
			
			System.out.println("UnegisterHandler: " + completeClientKey);
			if(clients.contains(completeClientKey)){
				clients.remove(completeClientKey);
				exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
			}else{
				exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0);
			}
		}
		finally{
			if(exchange!= null){
				exchange.close();
			}
		}
		
	}
	
}


