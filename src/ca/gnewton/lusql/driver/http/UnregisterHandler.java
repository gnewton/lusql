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
			System.out.println("UnegisterHandler: " + remoteHost);
			if(clients.contains(remoteHost)){
				clients.remove(remoteHost);
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


