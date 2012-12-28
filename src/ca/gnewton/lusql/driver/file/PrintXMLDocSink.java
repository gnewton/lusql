package ca.gnewton.lusql.driver.file;
import ca.gnewton.lusql.core.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class PrintXMLDocSink here.
 *
 *
 * Created: Sat Dec 20 23:07:13 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class PrintXMLDocSink 
	extends AbstractDocSink
{
	public String description()
	{
		return "Sink that prints out the documents in a simple XML format. Is also gzipped";
	}

	/**
	 * Describe indexName here.
	 */
	private String indexName;

	/**
	 * Creates a new <code>PrintXMLDocSink</code> instance.
	 *
	 */
	public PrintXMLDocSink() 
	{
	    
	}

	int addDocHintSize = 100;
	public int getAddDocSizeHint()
	{
		return addDocHintSize;
	}

	//Writer output = null;
	BufferedOutputStream output = null;
	public void init(MultiValueProp p) throws PluginException
	{
		setSupportsWritingToStdout(true);
	    
		extractProperties(p);	    
		if(getIndexName() == null)		    
			throw new PluginException("PrintXMLDocSink: Missing index name");
		try
			{

				//output = new BufferedWriter(new FileWriter(new File(getIndexName())));
				if(getWritingToStdout() || true)
					{
						output = new BufferedOutputStream(
						                                  System.out
						                                  //new GZIPOutputStream(System.out,
						                                  //1024*1024)
						                                  );
					}
				else
					{
						output = new BufferedOutputStream(
						                                  new GZIPOutputStream(
						                                                       new FileOutputStream(getIndexName() + ".xml.gz") 
						                                                       , 1024*1024)
						                                  );
					}
				output.write("<collection>\n".getBytes());
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
		try
			{
				output.write("\n</collection>\n".getBytes());
				if(output != null)
					output.close();
			}
		catch(Throwable t)
			{
				throw new PluginException();
			}

	}

	static String record = "r";

	Lock l = new ReentrantLock();

	public void addDoc(Doc[] docList)  throws DocSinkException
	{
		l.lock();
		try {
			for(Doc doc: docList)
				{
					//System.err.println("Sink");

					//FileWriter always assumes default encoding is OK!
					Map<String, List<String>> fields = doc.getFields();
					Iterator<String> it = fields.keySet().iterator();
					StringBuilder sb = new StringBuilder("<" + record + ">\n");		
					while(it.hasNext())
						{
							String key = it.next(); 
							//sb.append(key); 
							List<String>values = fields.get(key);
							for(String v: values)
								{
									sb.append("  <" + key.trim() + ">" + (v==null?v:clean(v.trim())) 
									          + "</" + key.trim() + ">\n");
									//System.err.println("Sink:" + v);
					
								}
						}
					sb.append("</" + record + ">\n");
					if(output != null)
						//output.write(sb.toString());
						output.write(sb.toString().getBytes());
					else
						System.out.println(sb.toString());
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
	    

	public Object internal()  throws DocSinkException
	{
		return null;
	}

	public boolean isThreaded()
	{
		return false;
	}

	public boolean isRemoveOnDone()
	{
		return false;
	}

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
		if(p.containsKey(LuSqlFields.SinkLocationKey))
			setIndexName(p.getProperty(LuSqlFields.SinkLocationKey).get(0));
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

	String clean(String s)
	{
		s = s.replaceAll("<", "&lt;");
		s = s.replaceAll(">", "&gt;");
		return s;
	}

	public String descriptor()
	{
		return this.getClass().getName() + ": File: " + getIndexName();
	}

}
