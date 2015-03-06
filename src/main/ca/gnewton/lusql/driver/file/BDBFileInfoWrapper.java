package ca.gnewton.lusql.driver.file;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.util.*;


/**
 * Describe class BDBFileInfoWrapper here.
 *
 *
 * Created: Mon Apr 19 16:38:41 EDT 2010
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
@Entity
public class BDBFileInfoWrapper 
{
    /**
     * Describe id here.
     */
    @PrimaryKey
    private String filePath;

    public BDBFileInfoWrapper()
	{
	    
	}

    /**
     * Get the <code>Id</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getFilePath() {
	return this.filePath;
    }

    /**
     * Set the <code>Id</code> value.
     *
     * @param newId The new Id value.
     */
    public final void setFilePath(final String newFilePath) {
	this.filePath = newFilePath;
    }
}
