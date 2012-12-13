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

    /**
     * Describe chunkHint here.
     */
    private int chunkHint = 10;

    /**
     * Describe threaded here.
     */
    private boolean threaded;

    /**
     * Creates a new <code>AbstractDocSource</code> instance.
     *
     */
    public AbstractDocSource() {

    }

    boolean supportsCompression = false;
    
    public void setSupportsCompression(boolean newS)
    {
	supportsCompression = newS;
    }
    

    public boolean supportsCompression()
	{
	    return false;
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

    boolean threadSafe = false;
    
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

    boolean supportsReadingFromStdin = false;
    
    public void setSupportsReadingFromStdin(boolean b)
    {
	supportsReadingFromStdin=b;
    }
    

    public boolean isSupportsReadingFromStdin()
    {
	return 	supportsReadingFromStdin;
    }
    
    boolean readingFromStdin = false;
    
    public void setReadingFromStdin(boolean b)
    {
	readingFromStdin = b;
    }
    
    public boolean getReadingFromStdin()
    {
	return readingFromStdin;
    }

    /**
     * Get the <code>Threaded</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isThreaded() {
	return threaded;
    }

    /**
     * Set the <code>Threaded</code> value.
     *
     * @param newThreaded The new Threaded value.
     */
    public final void setThreaded(final boolean newThreaded) {
	this.threaded = newThreaded;
    }
}
