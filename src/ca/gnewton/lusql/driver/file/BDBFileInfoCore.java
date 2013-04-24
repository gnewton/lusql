package ca.gnewton.lusql.driver.file;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.sleepycat.je.*;
import com.sleepycat.persist.*;

import ca.gnewton.lusql.core.*;

/**
 * Describe class BDBFileInfoCore here.
 *
 *
 * Created: Mon Apr 19 16:45:08 EDT 2010
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class BDBFileInfoCore 
{

    /**
     * Describe readOnly here.
     */
    private boolean readOnly=true;
    Environment myDbEnv = null;
    EnvironmentConfig envConfig = null;
    EntityStore store = null;
    EntityStore metaStore = null;
    BDBFileInfoWrapperDA bda = null;

    public static final String StoreName = "LuSqlStore";
    /**
     * Creates a new <code>BDBFileInfoCore</code> instance.
     *
     */

    public boolean isNew = false;
    public BDBFileInfoCore(String dbDirName, boolean allowCreate, boolean overWrite, boolean newReadOnly) 
    //throws DatabaseException
	{
	    setReadOnly(newReadOnly);
	    envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(allowCreate);
	    envConfig.setReadOnly(readOnly);
	    envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "200000000");
	    envConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, "7");

	    File dbDir = null;
	    try
	    {
		dbDir = new File(dbDirName);
		if(!dbDir.exists())
		    isNew = true;
		if(dbDir.exists() && overWrite)
		    {
			Util.removeDir(dbDirName);
			dbDir = new File(dbDirName);
			dbDir.mkdir();
		    }
		else
		    dbDir.mkdir();
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
	    bda = new BDBFileInfoWrapperDA(store);
	}
    
    public boolean containsKey(String filePath)
	throws DatabaseException
    {
	return (bda.pIdx.get(filePath) != null);
    }

    public void closeCursor(Iterator<BDBFileInfoWrapper> it)
	throws DatabaseException
	{
	    if(it != null && cursorsIterators.containsKey(it))
		cursorsIterators.get(it).close();
	}

    public void done() throws PluginException
	{
	    try
	    {
		Iterator<EntityCursor<BDBFileInfoWrapper>> it = cursorsIterators.values().iterator();
		while(it.hasNext())
		    it.next().close();

		for(EntityCursor<BDBFileInfoWrapper> cursor: cursors)
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

    private final ReentrantLock lock = new ReentrantLock();
    public void put(BDBFileInfoWrapper dw)
	throws DatabaseException
	{
	    lock.lock();
	    try
	    {
		bda.pIdx.put(dw);
	    }
	    finally
	    {
		lock.unlock();
	    }
	}


    PrimaryIndex<String,BDBFileInfoWrapper> pi = null;
    List<EntityCursor<BDBFileInfoWrapper>> cursors = new ArrayList<EntityCursor<BDBFileInfoWrapper>>();
    Map<Iterator<BDBFileInfoWrapper>, EntityCursor<BDBFileInfoWrapper>> cursorsIterators = 
       new HashMap<Iterator<BDBFileInfoWrapper>, EntityCursor<BDBFileInfoWrapper>>();

    public Iterator<BDBFileInfoWrapper> iterator()
	throws DatabaseException
	{
	    pi = store.getPrimaryIndex(String.class, BDBFileInfoWrapper.class); 
	    EntityCursor<BDBFileInfoWrapper> pi_cursor = pi.entities();
	    cursors.add(pi_cursor);
	    Iterator<BDBFileInfoWrapper> it = pi_cursor.iterator();
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
