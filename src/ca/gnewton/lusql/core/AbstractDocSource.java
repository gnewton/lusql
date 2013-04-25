package  ca.gnewton.lusql.core;
import java.util.*;

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

	public String showState(int n)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(ca.gnewton.lusql.util.Util.offset("DocSource: " + this.getClass().getName(),n) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("supports compression: " + supportsCompression(),n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("thread safe: " + isThreadSafe(),n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("supports reading from stdin: " + isSupportsReadingFromStdin(), n+1) + "\n");
		return sb.toString();
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
