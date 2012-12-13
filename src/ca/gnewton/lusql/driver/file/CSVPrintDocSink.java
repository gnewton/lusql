package ca.gnewton.lusql.driver.file;
import ca.gnewton.lusql.core.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.util.*;
import ca.gnewton.lusql.util.*;
import com.csvreader.CsvWriter;
import java.nio.charset.Charset;


public class CSVPrintDocSink 
    extends AbstractDocSink
{
    int maxDisplayFieldSize = 256;
    
    @Override
    public String description()
	{
	    return "Sink that prints out the docs as csv";
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
     * Creates a new <code>CSVPrintDocSink</code> instance.
     *
     */
    public CSVPrintDocSink() 
	{

	}


	CsvWriter writer = null;
	
    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    setSupportsWritingToStdout(true);
	    extractProperties(p);	    
	    try
	    {
		if(getIndexName() != null)
			writer = new CsvWriter(new FileOutputStream(getIndexName()), ',', Charset.forName("UTF-8"));
		
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
		if(writer != null)
		    writer.flush();
		    writer.close();
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
				//Map<String, List<String>> fields = doc.getFields();

				TreeMap<String, List<String>> fields = new TreeMap<String, List<String>>(doc.getFields());
				Iterator<String> it = fields.keySet().iterator();

			while(it.hasNext())
			    {
				String key = it.next();
		        System.out.println("key=" + key + "  value=" + fields.get(key).get(0));
				}
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
