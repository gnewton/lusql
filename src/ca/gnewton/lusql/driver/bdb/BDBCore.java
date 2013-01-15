package ca.gnewton.lusql.driver.bdb;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.sleepycat.je.*;
import com.sleepycat.persist.*;

import ca.gnewton.lusql.core.*;


/**
 * Describe class BDBCore here.
 *
 *
 * Created: Thu Dec 18 15:26:57 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class BDBCore 
{
    /**
     * Describe readOnly here.
     */
    private boolean readOnly=true;
    Environment myDbEnv = null;
    EnvironmentConfig envConfig = null;
    EntityStore store = null;
    EntityStore metaStore = null;
    BDBDocWrapperDA bda = null;

    public static final String PrimaryKeyPropKey="primaryKey";
    public static final String PrimaryKeyKey="primaryKeyKey";

    // fixxx check maybe should be settable
    public static final String StoreName = "LuSqlStore";
    /**
     * Creates a new <code>BDBCore</code> instance.
     *
     */

    // fixxx :: should be settable
    // BDB log files default to 50m
    long logFileMax = 1024l * 1024l * 50l;
    // fixxx :: should be settable
    int cleanerLookAheadCacheSize = 32 * 1024;
 
    public BDBCore(String dbDirName, boolean allowCreate, boolean newReadOnly) 
    //throws DatabaseException
	{
	    if(dbDirName== null)
		System.err.println("BDBCore Dir is null");
	    
	    setReadOnly(newReadOnly);
	    envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(allowCreate);
	    envConfig.setReadOnly(readOnly);
	    envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, Long.toString(logFileMax));

	    // Check to see what this does;
	    // envConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, Long.toString(cleanerLookAheadCacheSize));

	    // fixx check what this means and maybe should be settable
	    envConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, "7");
	    

	    File dbDir = null;
	    try
	    {
		dbDir = new File(dbDirName);
		if(allowCreate)
		    {
			if(dbDir.exists())
			    Util.removeDir(dbDirName);
			dbDir = new File(dbDirName);
			dbDir.mkdir();
		    }
	    }
	    catch(Throwable q)
	    {
		System.out.println("Problem with directory: " 
				   + dbDirName);
		q.printStackTrace();
		//throw new DatabaseException();
		throw new NullPointerException();
	    }
	    myDbEnv = new Environment(dbDir, envConfig);
	    StoreConfig storeConfig = new StoreConfig();
	    storeConfig.setAllowCreate(allowCreate);
	    storeConfig.setReadOnly(readOnly);
	    store = new EntityStore(myDbEnv, StoreName, storeConfig);
	    bda = new BDBDocWrapperDA(store);
	    map = bda.pIdx.map();
	}

    Map<String, BDBDocWrapper> map = null;

    public void closeCursor(Iterator<BDBDocWrapper> it)
	throws DatabaseException
	{
	    if(it != null && cursorsIterators.containsKey(it))
		cursorsIterators.get(it).close();
	}

    public void done() throws PluginException
	{
	    try
	    {
		Iterator<EntityCursor<BDBDocWrapper>> it = cursorsIterators.values().iterator();
		while(it.hasNext())
		    it.next().close();

		for(EntityCursor<BDBDocWrapper> cursor: cursors)
		{
		    if(cursor != null)
		    {
			try
			{
			    cursor.close();
			}
			catch(Throwable t)
			{

			}
		    }
		}
		if(store != null)
		    store.close();
		if (myDbEnv != null) 
		{    
		    if(!isReadOnly())
			myDbEnv.cleanLog();
		    myDbEnv.close();
		}
		
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	}


    public void put(BDBDocWrapper dw)
	throws DatabaseException
	{
	    bda.pIdx.put(dw);
	}


    public Doc makeDoc(BDBDocWrapper dw)
    {
	//fixxx: missing compressedFields and byteFields
	Doc d= new DocImp();
	d.setFields(dw.getFields());
	d.addProperty(PrimaryKeyKey, dw.getId());
	return d;
    }
    
    public Doc getDoc(String k)
    {
	    return makeDoc(get(k));
    }
    
    
    public BDBDocWrapper get(String key)
	    throws DatabaseException
	{
		return map.get(key);
	}

    public boolean containsKey(String key)
	throws DatabaseException
	{
	    return map.containsKey(key);
	}


    PrimaryIndex<String,BDBDocWrapper> pi = null;
    List<EntityCursor<BDBDocWrapper>> cursors = new ArrayList<EntityCursor<BDBDocWrapper>>();
    Map<Iterator<BDBDocWrapper>, EntityCursor<BDBDocWrapper>> cursorsIterators = 
       new HashMap<Iterator<BDBDocWrapper>, EntityCursor<BDBDocWrapper>>();

    public Iterator<BDBDocWrapper> iterator()
	throws DatabaseException
	{
	    pi = store.getPrimaryIndex(String.class, BDBDocWrapper.class); 
	    EntityCursor<BDBDocWrapper> pi_cursor = pi.entities();
	    cursors.add(pi_cursor);
	    Iterator<BDBDocWrapper> it = pi_cursor.iterator();
	    cursorsIterators.put(it, pi_cursor);
	    return it;
	}

    /**
     * Get the <code>ReadOnly</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isReadOnly() {
	return readOnly;
    }

    /**
     * Set the <code>ReadOnly</code> value.
     *
     * @param newReadOnly The new ReadOnly value.
     */
    public final void setReadOnly(final boolean newReadOnly) {
	this.readOnly = newReadOnly;
    }
}  //////////
