package ca.gnewton.lusql.driver.bdb;

import java.io.File;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.*;

/**
 * Describe class BDBDocWrapperDA here.
 *
 *
 * Created: Wed Dec 17 17:44:11 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class BDBDocWrapperDA 
{
    public PrimaryIndex<String,BDBDocWrapper> pIdx;

    /**
     * Creates a new <code>BDBDocWrapperDA</code> instance.
     *
     */
    public BDBDocWrapperDA(EntityStore store) 
	throws DatabaseException 
	{
	    // Primary key for SimpleEntityClass classes
	    pIdx = store.getPrimaryIndex(String.class, BDBDocWrapper.class);
	}
    
}
