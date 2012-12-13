package ca.gnewton.lusql.core;

/**
 * Describe class AbstractDocSink here.
 *
 *
 * Created: Fri May  1 23:48:22 2009
 *
 * @author <a href="mailto:gnewton4@">Glen Newton</a>
 * @version 1.0
 */
abstract public class AbstractDocSink 
    implements DocSink
{

    /**
     * Describe supportsWritingToStdout here.
     */
    private boolean supportsWritingToStdout = false;
    boolean supportsCompression = false;
    
    public boolean requiresPrimaryKeyField()
	{
	    return false;
	}

    boolean threaded = false;
    public boolean isThreaded()
	{
	    return threaded;
	}
    public void setThreaded(boolean tr)
	{
	    threaded = tr;
	}

    boolean threadSafe = true;
    public boolean isThreadSafe()
	{
	    return threadSafe;
	}
    public void setThreadSafe(boolean ts)
	{
	    threadSafe = ts;
	}


    public boolean isRemoveOnDone()
	{
	    return false;
	}
    public boolean isSupportsCompression()
	{
	    return supportsCompression;
	} 

    public void setSupportsCompression(boolean s)
	{
	     supportsCompression = s;
	} 

    /**
     * Get the <code>SupportsWritingToStdout</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isSupportsWritingToStdout() {
	return supportsWritingToStdout;
    }

    /**
     * Set the <code>SupportsWritingToStdout</code> value.
     *
     * @param newSupportsWritingToStdout The new SupportsWritingToStdout value.
     */
    public final void setSupportsWritingToStdout(final boolean newSupportsWritingToStdout) {
	this.supportsWritingToStdout = newSupportsWritingToStdout;
    }

    boolean writingToStdout = false;
    
    public void setWritingToStdout(boolean b)
    {
	writingToStdout = b;
    }

    public boolean getWritingToStdout()
    {
	return writingToStdout;
    }

    public String showState(int n)
	throws PluginException    
    {
	StringBuilder sb = new StringBuilder();
	sb.append(ca.gnewton.lusql.util.Util.offset("DocSink: " + this.getClass().getName(),n) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("is threaded: " + isThreaded(),n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("remove on done: " + isRemoveOnDone(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("supports compression: " + isSupportsCompression(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("supports writing to stdout: " + isSupportsWritingToStdout(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("requires primary key field: " + requiresPrimaryKeyField(), n+1) + "\n");
	return sb.toString();
    }
    
    
}
