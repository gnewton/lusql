package ca.gnewton.lusql.driver.solrj;
import java.io.*;
import java.util.Properties;
import java.lang.reflect.Constructor;
import java.util.*;

import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.common.*;
import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.util.concurrent.locks.*;

/**
 * Describe class SolrjDocSink here.
 *
 *
 * Created: Sun Jan 18 01:57:29 2009
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class SolrjDocSink 
    extends AbstractDocSink
{
    @Override
    public String description()
	{
	    return "Sink that writes to Solr via SolrJ";
	} 

    /**
     * Creates a new <code>SolrjDocSink</code> instance.
     *
     */
    public SolrjDocSink() 
    {
	setWritingToStdout(false);
    }

    protected SolrServer server = null;

    public void init(MultiValueProp p) throws PluginException
	{
	    try
	    {
		// From http://wiki.apache.org/solr/Solrj
		String url = "http://localhost:8983/solr";
		server = new CommonsHttpSolrServer( url );
		CommonsHttpSolrServer cserver = (CommonsHttpSolrServer)server;
		cserver.setSoTimeout(1000);  // socket read timeout
		cserver.setConnectionTimeout(100);
		cserver.setDefaultMaxConnectionsPerHost(100);
		cserver.setMaxTotalConnections(100);
		cserver.setFollowRedirects(false);  // defaults to false
		cserver.setAllowCompression(true);
		cserver.setMaxRetries(1); 
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	}

    @Override
    public Properties explainProperties()
	{
	    return null;
	}

    public void done() throws PluginException
	{
	    try
	    {
		this.commit();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	    
	}

    Collection<SolrInputDocument> docs = null;
    int docCount = 0;
    private final ReentrantLock lock = new ReentrantLock();

    public void addDoc(Doc[] docList)  throws DocSinkException
	{

	    try
	    {
		for(Doc doc: docList)
		    {
			SolrInputDocument document = makeSolrDocument(doc);		
			docs.add(document);
			lock.lock();
			try
			    {
				docCount++;
				if(docCount > 100)
				    {
					// Need to thread this
					server.add(docs);
					docs.clear();
					docCount = 0;
				    }
			    }
			finally
			    {
				lock.unlock();
			    }
		    }
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new DocSinkException();
	    }


	    
	    SolrInputDocument doc1 = new SolrInputDocument();
	    doc1.addField( "id", "id1", 1.0f );
	    doc1.addField( "name", "doc1", 1.0f );

	}

    SolrInputDocument makeSolrDocument(Doc doc)
	{
	    SolrInputDocument document = new SolrInputDocument();

	    Iterator<String> it = doc.getFieldNames();
	    while(it.hasNext())
	    {
		String fieldName = it.next();
		List<String> values = doc.getFieldValues(fieldName);
		if(values != null)
		{
		    Iterator<String> iValues = values.iterator();
		    while(iValues.hasNext())
		    {
			String v = iValues.next();
			if(v != null)
			{
			    document.addField(fieldName, v, 1.0f);
			}
		    }
		}
	    }
	    return document;
	}

    public Object internal()  throws DocSinkException
	{
	    return null;
	}

    public boolean isThreaded()
	{
	    return false;
	}

    public boolean isRemoveOnDone()
	{
	    return false;
	}

    public void commit() throws DocSinkException
	{
	    try
	    {
		server.commit();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new DocSinkException();
	    }
	}

    public String descriptor()
	{
	    return this.getClass().getName(); 
	}

}
