package ca.gnewton.lusql.driver.json;

import ca.gnewton.lusql.core.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.util.*;
import ca.gnewton.lusql.util.*;

public class JsonDocSink 
    extends AbstractDocSink
{

    public JsonDocSink() 
	{

	}

    public String description()
	{
		return "Sink that prints out the documents and their fields/values as json.";
	}

    @Override
    public boolean requiresPrimaryKeyField()
	{
	    return false;
	}

    public int getAddDocSizeHint()
	{
		return 100;
	}


    Writer output = null;

    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    setSupportsWritingToStdout(true);
	    extractProperties(p);	    
	    try
	    {
		if(getIndexName() != null)
		    output = new BufferedWriter(new FileWriter(new File(getIndexName())));
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

    @Override
    public void done() throws PluginException
	{
	    try
	    {
		if(output != null)
		    output.close();
	    }
	    catch(Throwable t)
	    {
		throw new PluginException();
	    }

	}


    Lock l = new ReentrantLock();
    int count = 0;

    @Override
    public void addDoc(Doc[] docList)  throws DocSinkException
	{

	    for(Doc doc: docList)
		{
		    l.lock();		    
		    try {
			    String s = Doc2Json.doc2json(doc);
			    if(output != null)
				    output.write(s);
			    else
				    System.out.println(s);
		    }
		    catch(Throwable t)
			    {
				    t.printStackTrace();
			    throw new DocSinkException();
			    }
		    finally 
			    {
				    l.unlock();
			    }
		}
	}
	

    public Object internal()  throws DocSinkException
	{
	    return null;
	}

    @Override
    public boolean isThreaded()
	{
	    return false;
	}
    @Override
    public boolean isRemoveOnDone()
	{
	    return false;
	}

    @Override
    public void commit() throws DocSinkException
	{

	}


    void extractProperties(MultiValueProp p)
	{
	    if(p.containsKey("index"))
		setIndexName(p.getProperty("index").get(0));
	}

	String indexName = null;
	
    public final String getIndexName() {
	return indexName;
    }

    public final void setIndexName(final String newIndexName) {
	this.indexName = newIndexName;
    }


}
