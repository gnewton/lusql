package ca.gnewton.lusql.driver.file;

import java.io.File;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.*;

/**
 * Describe class BDBFileInfoWrapperDA here.
 *
 *
 * Created: Mon Apr 19 16:45:08 EDT 2010
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class BDBFileInfoWrapperDA 
{
    public PrimaryIndex<String,BDBFileInfoWrapper> pIdx;

    /**
     * Creates a new <code>BDBDocWrapperDA</code> instance.
     *
     */
    public BDBFileInfoWrapperDA(EntityStore store) 
	throws DatabaseException 
	{
	    // Primary key for SimpleEntityClass classes
	    pIdx = store.getPrimaryIndex(String.class, BDBFileInfoWrapper.class);
	}
    
}
