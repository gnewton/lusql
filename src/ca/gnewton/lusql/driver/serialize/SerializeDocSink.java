package ca.gnewton.lusql.driver.serialize;
import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.util.concurrent.locks.*;

import org.apache.lucene.store.*;
import com.sleepycat.je.*;
import com.sleepycat.persist.*;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;


public class SerializeDocSink 
    extends AbstractDocSink
{
    public String description()
	{
	    return "Sink that serialized LuSql's internal Doc object to disk.";
	} 
    /**
     * Describe indexDirName here.
     */
    private String indexDirName;

    /**
     * Describe create here.
     */
    private boolean create;


    int addDocHintSize = 100;
    public int getAddDocSizeHint()
	{
	    return addDocHintSize;
	}

    public SerializeDocSink()
    {
	setWritingToStdout(true);
    }
    

    ObjectOutputStream oo;
    
    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    extractProperties(p);
	    try
	    {
		// Open the environment.
		try 
		{
		    oo = new ObjectOutputStream(
		                                new GZIPOutputStream(
		                                                     new FileOutputStream(getIndexDirName()) 
		                                                     ,1024*32)
			);
		} 
		catch (Throwable t) 
		{
		    t.printStackTrace();
		    throw new PluginException("Problem instantiating BDB environment/db");
		}
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
		System.out.println("Shutting down Serialize");
		oo.flush();
		oo.close();
		System.out.println("Done shutting down Serialize");
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace(); 
		throw new PluginException();
	    }
	}
    
    private final ReentrantLock lock = new ReentrantLock();
    static int count = 0;
    public void addDoc(Doc[] docList)  throws DocSinkException
	{
	    try
	    {
		lock.lock();
		try
		{
		    for(Doc doc: docList)
			{
			    count++;
			    oo.writeUnshared(doc);
			    oo.reset();
			    if(count%100 == 0)
				oo.flush();
			}
		}
		finally 
		{
		    lock.unlock();
		}
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		//System.out.println(doc);
		throw new DocSinkException();
	    }
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
		oo.flush();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		//System.out.println(doc);
		throw new DocSinkException();
	    }
	}


    void extractProperties(MultiValueProp  p)
	{
	    if(p.containsKey(LuSqlFields.CreateSinkKey))
		setCreate(Boolean.parseBoolean(p.getProperty(LuSqlFields.CreateSinkKey).get(0)));

	    if(p.containsKey(LuSqlFields.SinkLocationKey))
		setIndexDirName(p.getProperty(LuSqlFields.SinkLocationKey).get(0));
	}

    public String descriptor()
	{
	    return this.getClass().getName() + ": Directory: " + getIndexDirName();
	}


    /**
     * Get the <code>IndexDirName</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getIndexDirName() {
	return indexDirName;
    }

    /**
     * Set the <code>IndexDirName</code> value.
     *
     * @param newIndexDirName The new IndexDirName value.
     */
    public final void setIndexDirName(final String newIndexDirName) {
	this.indexDirName = newIndexDirName;
    }

    /**
     * Get the <code>Create</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isCreate() {
	return create;
    }

    /**
     * Set the <code>Create</code> value.
     *
     * @param newCreate The new Create value.
     */
    public final void setCreate(final boolean newCreate) {
	this.create = newCreate;
    }


}
