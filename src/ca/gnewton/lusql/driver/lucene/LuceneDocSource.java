package ca.gnewton.lusql.driver.lucene;
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.locks.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.store.Directory.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class LuceneDocSource here.
 *
 *
 * Created: Mon Dec 22 13:59:00 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class LuceneDocSource 
	extends LuceneCore
	implements DocSource, Gettable
{
	/**
	 * Describe chunkHint here.
	 */
	private int chunkHint = 5;

	public String description()
	{
		return "Source that reads Documents from a Lucene index";
	} 
 
	/**
	 * Describe supportsCompression here.
	 */
	private boolean supportsCompression = false;
	{
		debug = true;
	}
	boolean search = false;
	/**
	 * Creates a new <code>LuceneDocSource</code> instance.
	 *
	 */
	public LuceneDocSource() 
	{
		// OK
	}

	static
	{
		System.setProperty("org.apache.lucene.FSDirectory.class",
		                   NIOFSDirectory.class.getName());
	}

	int count = 0;
	int numDocs = -1;

	IndexReader reader = null;
	FieldSelector fieldSelector = null;
    
	@Override
	public void init(MultiValueProp p) throws PluginException
	{
		try
			{
				extractProperties(p);
				if(getQuery() != null && getQuery().equals("*"))
					search = true;
		
				//IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, ana).setRAMBufferSizeMB(getRAMBufferSize());
				reader = openIndex(getSourceLocation());
				
				numDocs = reader.numDocs();
				if(fields != null)
					{
						String[] wantedFields = new String[fields.size()];
						Iterator<String>it = fields.iterator();
						int n = 0;
						while(it.hasNext())
							{
								wantedFields[n++] = it.next();
							}
						fieldSelector = new MapFieldSelector(wantedFields);
					}
				if(search)
					{
						// TODO doSearch();
					}
			}
		catch(Throwable t)
			{
				//printDefaults();
				t.printStackTrace();
				throw new PluginException();
			}
	}	    

	private static final IndexReader openIndex(String location)
		throws IOException, CorruptIndexException
	{
		return IndexReader.open(FSDirectory.open(new File(location)));
	}
	

	public Properties explainProperties()
	{
		return null;
	}

	public void done() throws PluginException
	{
		try
			{
				reader.close();
			}
		catch(Throwable t)
			{
				throw new PluginException("LucenDocSource: Problem closing lucene index: " 
				                          + getSourceLocation());
			}
	}


	public Doc next()  throws DataSourceException
	{
		Doc doc = null;
		try
			{
				//if(search)
				{
					// TODO return nextSearchResult();
					//return nextDoc();
				}
				//else
				{
					doc = nextDoc();
					if(doc == null)
						return new DocImp().setLast(true);	
					else
						return doc;
				}
			}
		catch(Throwable t)
			{
				t.printStackTrace();
				throw new DataSourceException();
			}
	}
    
	void extractProperties(MultiValueProp p)
	{
		luceneExtractProperties(p);
	}


	//private final ReentrantLock lock = new ReentrantLock();
	Doc nextDoc()
		throws CorruptIndexException, IOException
	{
		// not needed: single threaded...
		//lock.lock();
		Document document = null;
		//try
		{
			if(count<numDocs)
				{
					document = reader.document(count, fieldSelector);
				}
			else 
				{
					LuSql.cleanHalt();
					return null;
				}
			count++;
		}
		//finally
		{
			//lock.unlock();
		}
		return makeDoc(document);
	}

	Doc makeDoc(Document d)
	{
		Doc doc = new DocImp();
		Iterator<Fieldable>it = d.getFields().iterator();
		while(it.hasNext())
			{
				Fieldable field = it.next();
				String sName = field.name();
				if(fields != null && !fields.contains(sName))
					continue;
				String[] values = d.getValues(sName);
				for(int i=0; i<values.length; i++)
					doc.addField(sName, values[i]);
			}
		return doc;
	}

	Set<String>fields = null;
	// These are fields want
	public void addField(final String field)
	{
		if(fields == null)
			fields = new HashSet<String>();
		fields.add(field);
		//System.out.println("************  LuceneDocSource. adding: " + field);
	}

	/**
	 * Get the <code>SupportsCompression</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean supportsCompression() {
		return supportsCompression;
	}

	/**
	 * Set the <code>SupportsCompression</code> value.
	 *
	 * @param newSupportsCompression The new SupportsCompression value.
	 */
	public final void setSupportsCompression(final boolean newSupportsCompression) {
		this.supportsCompression = newSupportsCompression;
	}

	public boolean isThreadSafe()
	{
		return false;
	}


	/**
	 * Get the <code>ChunkHint</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getChunkHint() {
		return chunkHint;
	}

	/**
	 * Set the <code>ChunkHint</code> value.
	 *
	 * @param newChunkHint The new ChunkHint value.
	 */
	public final void setChunkHint(final int newChunkHint) {
		this.chunkHint = newChunkHint;
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

	public String showState(int n)
		throws PluginException    
	{
		StringBuilder sb = new StringBuilder();
		sb.append(ca.gnewton.lusql.util.Util.offset("DocSink: " + this.getClass().getName(),n));
		return sb.toString();
	}

	boolean threadSafe = false;
    
	public void setThreadSafe(final boolean newThreadSafe)
	{
		this.threadSafe = newThreadSafe;
	}    


	boolean threaded = false;
    
	/**
	 * Set the <code>Threaded</code> value.
	 *
	 * @param newThreaded The new Threaded value.
	 */
	public final void setThreaded(final boolean newThreaded) {
		this.threaded = newThreaded;
	}

	public Doc get(String field, String key) throws DataSourceException
	{
		return null;
	}

}
