package ca.gnewton.lusql.driver.file;
import ca.gnewton.lusql.core.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.util.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class PrintDocSink here.
 *
 *
 * Created: Sat Dec 20 23:07:13 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class PrintDocSink 
    extends AbstractDocSink
{
    int maxDisplayFieldSize = 256;
    
    @Override
    public String description()
	{
	    return "Sink that prints out the documents and their fields/values. Useful for validation of Documents/fields";
	}

    @Override
    public boolean requiresPrimaryKeyField()
	{
	    return false;
	}
    /**
     * Describe indexName here.
     */
    private String indexName;

    int addDocHintSize = 100;
    public int getAddDocSizeHint()
	{
	    return addDocHintSize;
	}

    /**
     * Creates a new <code>PrintDocSink</code> instance.
     *
     */
    public PrintDocSink() 
	{

	}


    Writer output = null;

    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    setSupportsWritingToStdout(true);
	    extractProperties(p);	    
	    try
	    {
		if(getIndexName() != null)
		    output = new BufferedWriter(new FileWriter(new File(getIndexName())));
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	}

    @Override
    public Properties explainProperties()
	{
	    return null;
	}

    @Override
    public void done() throws PluginException
	{
	    try
	    {
		if(output != null)
		    output.close();
	    }
	    catch(Throwable t)
	    {
		throw new PluginException();
	    }

	}


    Lock l = new ReentrantLock();
    int count = 0;

    @Override
    public void addDoc(Doc[] docList)  throws DocSinkException
	{

	    for(Doc doc: docList)
		{
		    l.lock();		    
		    try {
			//FileWriter always assumes default encoding is OK!

			Map<String, List<String>> fields = doc.getFields();
			Iterator<String> it = fields.keySet().iterator();
			StringBuilder sb = new StringBuilder();		
			while(it.hasNext())
			    {
				String key = it.next();
				//sb.append(key);
				List<String>values = fields.get(key);
				for(String v: values)
				    {
					if(v!= null && v.length() > maxDisplayFieldSize-1)
					    sb.append(key.trim() 
						      + ": " 
						      + (v==null?v:v.substring(0, maxDisplayFieldSize).trim()) 
						      + "...\n");
					else
					    sb.append(key.trim() 
						      + ": " 
						      + (v==null?v:v.trim()) 
						      + "\n");
				    }
			    }
			sb.append("#-----------------------------------------------------------------#");
			sb.append("\n<" + count + ">");
			count++;
			if(output != null)
			    output.write(sb.toString());
			else
			    System.out.println(sb.toString());
		    }
		    catch(Throwable t)
			{
			    t.printStackTrace();
			    throw new DocSinkException();
			}
		    finally 
			{
			    l.unlock();
			}
		}
	}
    	    

    public Object internal()  throws DocSinkException
	{
	    return null;
	}

    @Override
    public boolean isThreaded()
	{
	    return false;
	}
    @Override
    public boolean isRemoveOnDone()
	{
	    return false;
	}

    @Override
    public void commit() throws DocSinkException
	{

	}
    public void setRemoveOnDone(boolean b)
	{

	}
    public void setPrimaryKeyField(String f)
	{

	}

    void extractProperties(MultiValueProp p)
	{
	    if(p.containsKey("index"))
		setIndexName(p.getProperty("index").get(0));
	    if(p.containsKey("fieldSize"))
		maxDisplayFieldSize = Integer.parseInt(p.getProperty("fieldSize").get(0));
	}

    /**
     * Get the <code>IndexName</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getIndexName() {
	return indexName;
    }

    /**
     * Set the <code>IndexName</code> value.
     *
     * @param newIndexName The new IndexName value.
     */
    public final void setIndexName(final String newIndexName) {
	this.indexName = newIndexName;
    }


}
