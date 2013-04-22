package ca.gnewton.lusql.driver.bdb;

import com.sleepycat.je.*;
import com.sleepycat.persist.*;
import java.util.*;
import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;

public class BDBDocSource 
    extends AbstractDocSource
	implements Gettable
{
    public String description()
	{
	    return "Source that read documents to Berkely DB. Note that only BDB databases created by ca.gnewton.lusql.driver.bdb.BDBDocSink can be read.";
	}
    /**
     * Describe indexDirName here.
     */
    private String indexDirName;


    BDBCore core = null;
    /**
     * Creates a new <code>BDBDocSource</code> instance.
     *
     */
    public BDBDocSource() {


    }

    public Iterator<BDBDocWrapper> iterator = null;
    public Doc next()  throws DataSourceException
	{
	    //System.err.println("BDBDocSource: next");
	    
	    if(core == null)
		throw new NullPointerException("Core = null. Did you call init first?");
	    if(iterator == null)
	    {
		try
		{
		    iterator = core.iterator();
		}
		catch(DatabaseException t)
		{
		    t.printStackTrace();
		    throw new DataSourceException("Problem instantiating BDB iterator");
		}
	    }


	    if(!iterator.hasNext())
		return new DocImp().setLast(true);	
		
	    //System.err.println("BDBDocSource: doc+");
	    Doc doc = new DocImp();
	    Map<String, List<String>> docFields = iterator.next().getFields();
	    if(docFields == null)
		{
		    doc.setFields(docFields);
		    System.err.println("BDBDocSource: NOFIELDS?");
		}
	    
	    else
		{
		    Iterator<String> fieldIt = docFields.keySet().iterator();
		    while(fieldIt.hasNext())
			{
			    String field = fieldIt.next();
			    //System.out.println("Field=" + field);
			    
			    if(docFields.containsKey(field))
				{
				    for(String value: docFields.get(field))
					{
					    doc.addField(field, value);
					    //System.out.println("\tvalue=" + value);
					}
				}
			}
		}
	    return doc;
	}


    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    setSupportsCompression(false);
	    extractProperties(p);
	    try
	    {
		// Open the environment.
		try 
		{
		    core = new BDBCore(getIndexDirName(), false, true);
		    //System.err.println("BDBDocSource: opeining " + getIndexDirName());
		    
		} 
		catch (Throwable t) 
		{
		    t.printStackTrace();
		    throw new PluginException("Problem instantiating BDB environment/db");
		}
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	}

    public Properties explainProperties()
	{
	    return null;
	}

    public void done() throws PluginException
	{
	    core.done();
	}

    void extractProperties(MultiValueProp p)
	{
	    if(p.containsKey(LuSqlFields.SourceLocationKey))
		    setIndexDirName(p.getProperty(LuSqlFields.SourceLocationKey).get(0));
	}

    /**
     * Get the <code>IndexDirName</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getIndexDirName() {
	return indexDirName;
    }

    /**
     * Set the <code>IndexDirName</code> value.
     *
     * @param newIndexDirName The new IndexDirName value.
     */
    public final void setIndexDirName(final String newIndexDirName) {
	this.indexDirName = newIndexDirName;
    }

    Set<String>fields = null;
    public void addField(final String field)
	{
	    if(fields == null)
		fields = new HashSet<String>();
	    fields.add(field);
	}

    public boolean isThreadSafe()
	{
	    return false;
	}

    public String showState(int n)
    {
	StringBuilder sb = new StringBuilder();
	sb.append(ca.gnewton.lusql.util.Util.offset("BDBDocDocSource",n));
	sb.append(ca.gnewton.lusql.util.Util.offset("index dir name:" + indexDirName,n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("supports compression:" + supportsCompression(),n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("storeName:" + BDBCore.StoreName, n+1));
	return sb.toString();
	
    }

	@Override
	public Doc get(String field, String key)
		throws DataSourceException
	{
		if(core == null){
			throw new DataSourceException("BDBCore is null");
		}
		return core.getDoc(key);
	}

	@Override
    public boolean containsKey(String key)
	    throws DatabaseException
	{
	    return core.containsKey(key);
	}	

}//

