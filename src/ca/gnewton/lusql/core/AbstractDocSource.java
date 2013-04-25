package  ca.gnewton.lusql.core;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;


/**
 * Describe class AbstractDocSource here.
 *
 *
 * Created: Fri May  1 23:51:34 2009
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
abstract public class AbstractDocSource 
	implements DocSource
{
	private final static Logger log = Logger.getLogger(AbstractDocSource.class.getName()); 
    static
    {
	    BasicConfigurator.configure();
    }


	private int chunkHint = 10;
	private boolean threaded = false;
	boolean supportsCompression = false;
	boolean threadSafe = false;
	boolean supportsReadingFromStdin = false;
	boolean readingFromStdin = false;

	public AbstractDocSource() {

	}

	public void setSupportsCompression(boolean supportsCompression)
	{
		this.supportsCompression = supportsCompression;
	}
    

	public boolean supportsCompression()
	{
		return supportsCompression;
	}

	@Override
	public void showState() throws PluginException
	{
	    Util.showState(this, log);

	    log.info("Supports reading from stdin: " + isSupportsReadingFromStdin());
	}


    
	public void setThreadSafe(final boolean newThreadSafe)
	{
		this.threadSafe = newThreadSafe;
	}
    

	public boolean isThreadSafe()
	{
		return threadSafe;
	}
	public void addField(String field)
	{
	
	}

	public void setSupportsReadingFromStdin(boolean b)
	{
		supportsReadingFromStdin=b;
	}
    

	public boolean isSupportsReadingFromStdin()
	{
		return 	supportsReadingFromStdin;
	}
    

    
	public void setReadingFromStdin(boolean b)
	{
		readingFromStdin = b;
	}
    
	public boolean getReadingFromStdin()
	{
		return readingFromStdin;
	}

	public final boolean isThreaded() {
		return threaded;
	}

	public final void setThreaded(final boolean newThreaded) {
		this.threaded = newThreaded;
	}

	public void done() throws PluginException
	{
		
	}

	private long maxDocs = Long.MAX_VALUE;
	public void setMaxDocs(final long maxDocs)
	{
		this.maxDocs = maxDocs;
	}
	
}
