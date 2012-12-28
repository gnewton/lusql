package ca.gnewton.lusql.driver.http;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.Compressor;
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.util.Queue;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class RegisterHandler implements HttpHandler
{
	Set<String>clients;

	public RegisterHandler() {
		clients = new HashSet<String>();
	}

	public void handle(HttpExchange exchange) throws IOException {
		try{
			String remoteHost = exchange.getRemoteAddress().getHostName();
			
			clients.add(remoteHost);
			System.out.println("RegisterHandler: " + remoteHost);
			
			exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
		}
		finally{
			exchange.close();
		}
	}

	public void setClients(final Set<String>clients)
	{
		this.clients = clients;
	}

	public Set<String>getClients()
	{
		return this.clients;
	}
	
	
}


