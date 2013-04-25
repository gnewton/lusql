package ca.gnewton.lusql.core;
import org.apache.lucene.document.*;
import java.util.*;
import ca.gnewton.lusql.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

/**
 * Describe class BaseFilter here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada, Glen Newton
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 *
 */
abstract public class BaseFilter 
    implements DocFilter
{
	private final static Logger log = Logger.getLogger(LuSqlMain.class.getName()); 
    static
    {
	    BasicConfigurator.configure();
    }

    private LuSql luSql;
    MultiValueProp properties = null;

    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    properties = p;
	}
  /**
   * Get the <code>Properties</code> value.
   *
   * @return a <code>Properties</code> value
   */
  public MultiValueProp getProperties() {
    return properties;
  }

  /**
   * Set the <code>Properties</code> value.
   *
   * @param newProperties The new Properties value.
   */
  public final void setProperties(final MultiValueProp newProperties) {
    this.properties = newProperties;
  }


    /**
     * Set the <code>LuSql</code> value.
     *
     * @param newLuSql The new LuSql value.
     */
    public final void setLuSql(final LuSql newLuSql) {
	this.luSql = newLuSql;
    }

    /**
     * Get the <code>LuSql</code> value.
     *
     * @return a <code>LuSql</code> value
     */
    public final LuSql getLuSql() {
	return luSql;
    }

    
    public void done()
	throws ca.gnewton.lusql.core.PluginException
	{

	}

    public Properties explainProperties()
	{
	    return null;
	}

    public String showState(int n)
    {
	StringBuilder sb = new StringBuilder();
	sb.append(ca.gnewton.lusql.util.Util.offset("Filter: " + this.getClass().getName(),n));
	return sb.toString();
    }

    public boolean isThreadSafe()
    {
	return true;
    }

    boolean threaded = false;
    boolean threadSafe = true;
    public boolean isThreaded()
    {
	return false;
    }
    
    public final void setThreaded(final boolean newThreaded) {
	this.threaded = newThreaded;
    }

    public void setThreadSafe(final boolean newThreadSafe)
    {
	this.threadSafe = newThreadSafe;
    }

	private long maxDocs = Long.MAX_VALUE;
	public void setMaxDocs(final long maxDocs)
	{
		this.maxDocs = maxDocs;
	}

	@Override
	public void showState() throws PluginException
	{
	    Util.showState(this, log);

	}

    
}
