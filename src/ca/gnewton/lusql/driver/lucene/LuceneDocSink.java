package ca.gnewton.lusql.driver.lucene;
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.locks.*;
import org.apache.log4j.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;

import ca.gnewton.lusql.util.*;

/**
 * Describe class LuceneIndex here.
 *
 *
 * Created: Mon Dec  1 16:09:02 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class LuceneDocSink 
    extends LuceneCore
    implements DocSink
{
    final static String compoundFormatKey = "compoundFormat";
    
	//static Category cat = Category.getInstance(LuSql.class.getName());
    public boolean requiresPrimaryKeyField()
	{
	    return false;
	}

    public String description()
	{
	    return "Sink that indexes Documents into Lucene";
	}
    /**
     * Describe supportsCompression here.
     */
    private boolean supportsCompression = false;

    /**
     * Describe compoundFormat here.
     */
    private boolean compoundFormat = true;

    /**
     * Creates a new <code>LuceneIndex</code> instance.
     *
     */
    public LuceneDocSink() {

    }

    int addDocHintSize = 1;
    public int getAddDocSizeHint()
	{
	    return addDocHintSize;
	}

    public void commit() throws DocSinkException
	{
	    try
	    {
		writer.commit();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new DocSinkException();
	    }
	}

    static
	{
	    System.setProperty("org.apache.lucene.FSDirectory.class",NIOFSDirectory.class.getName());
	}

	IndexWriter writer = null;
    public void init(MultiValueProp p) 
	throws PluginException
	{
	    try
	    {
		extractProperties(p);
		if(p.containsKey(LuceneDocSink.compoundFormatKey))
		    setCompoundFormat(Boolean.parseBoolean(p.getProperty(LuceneDocSink.compoundFormatKey).get(0)));
		
		Analyzer ana = newAnalyzer();
		if(ana == null)
		    throw new PluginException("Problem with Analyzer: null;  Should be: "
					      + getAnalyzerName());

		/*
		writer = new IndexWriter(makeDirectory(),
					 ana,
					 isIndexCreate(),
					 maxFieldLength
					   // fixxx 2010.07.08
					 );
		*/
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, ana).setRAMBufferSizeMB(getRAMBufferSize());
		
		
		writer = new IndexWriter(makeDirectory(), config);

		//FIXX
		//writer.setMaxFieldLength(999999);
	    }
	    catch(Throwable t)
	    {
		    //printDefaults();
		t.printStackTrace();
		throw new PluginException();
	    }
	}

    Directory makeDirectory()
	throws IOException
    {
	Set<String> exts = new HashSet<String>();
	exts.add("tvf");
	exts.add("tis");
	exts.add("tii");
	exts.add("frq");
	exts.add("nrm");
	
	return FSDirectory.open(new File(getLuceneIndexName()));

	/* FIXXX
	return new FileSwitchDirectory(exts,
				       FSDirectory.open(new File(getLuceneIndexName())),
				       true);
	*/
    }

    public Properties explainProperties()
	{
	    Properties p = new Properties();
	    p.setProperty(LuSqlFields.BufferSizeKey, "RAM Buffer size");
	    p.setProperty(LuSqlFields.CreateSinkKey, "Create index if does not exist");
	    p.setProperty(LuSqlFields.SinkLocationKey, "Name of index directory");
	    p.setProperty(LuSqlFields.AnalyzerClassKey, "Class name for the analyzer");
	    p.setProperty(LuSqlFields.StopWordFileNameKey, "Name of stop work file");
	    p.setProperty(LuSqlFields.RemoveSinksOnDoneKey, "Remove on done? boolean");
	    return p;
	}
    

    public void addDoc(Doc[] docList)  
	throws DocSinkException
	{
	    for(Doc doc: docList)
		{
		    if(doc == null)
			continue;
		    try
			{
			    Document document = makeDocument(doc);
			    if(writer == null)
				System.err.println("Writer is null");
			    writer.addDocument(document);
			}
		    
		    catch(Throwable t)
			{
			    System.err.println("Exception on " + getLuceneIndexName());
			    t.printStackTrace();
			    throw new DocSinkException("Failed addDocument");
			}
		}
	}

    public void done()  
	throws PluginException
	{
	    //TODO removeOnDone
	    if(isRemoveOnDone())
	    {
		    //cat.info("Removing Lucene index directory: " + getLuceneIndexName());
		    ca.gnewton.lusql.core.Util.removeDir(getLuceneIndexName());
	    }
	}

    Document makeDocument(Doc doc)
	{
	    Document document = new Document();
	    Iterator<String> it = doc.getFieldNames();

	    while(it.hasNext())
	    {
		String fieldName = it.next();
		//System.out.println(fieldName);
		
		List<String> values = doc.getFieldValues(fieldName);
		if(values != null)
		{
		    LuceneFieldParameters lpf = doc.getFieldParameters(fieldName);
		    if (lpf == null)
			lpf = new LuceneFieldParameters();
		    Iterator<String> iValues = values.iterator();
		    while(iValues.hasNext())
		    {
			String v = iValues.next();
			if(v != null)
			{
			    try
			    {
				//System.out.println("LuceneDocSink: makeDocument: value of lpf=" 
				//+ lpf);
				
				Field f = new Field(fieldName, v, lpf.getStore(), lpf.getIndex(), lpf.getTermVector());
				f.setBoost(doc.getBoost(fieldName));
				document.add(f);
			    }
			    catch(NullPointerException n)
			    {
				if(v.length() > 64)
				    v = v.substring(0,63);
				System.err.println("Error with field: " + fieldName + "=" + v);
				throw n;
			    }
			}

		    }
		} 
		else
		{
		    List<Reader> readerFields = doc.getFieldReaders(fieldName);
		    LuceneFieldParameters lpf = doc.getFieldParameters(fieldName);
		    if(readerFields != null)
		    {
			Iterator<Reader> itr = readerFields.iterator();
			while(itr.hasNext())
			{
			    Reader reader = itr.next();
			    //cat.info("LucenDocSink: using reader: " + reader);
			    try
			    {
				Field f = new Field(fieldName, reader, lpf.getTermVector());
				f.setBoost(doc.getBoost(fieldName));
				document.add(f);
			    }
			    catch(NullPointerException n)
			    {
				    //cat.info("Error with field: " + fieldName + " [reader field]");
				throw n;
			    }
			}
		    }
		}
	    }
	    return document;
	}


    public Object internal()
	{
	    return  writer;
	}

    void extractProperties(MultiValueProp p)
	{
	    luceneExtractProperties(p);
	}

    public String descriptor()
	{
	    return this.getClass().getName() 
		+ (
		   (getLuceneIndexName()==null)
		   ?""
		   :("Writing to location: " + getLuceneIndexName())
		   )
		;
	} 

    /**
     * Get the <code>SupportsCompression</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isSupportsCompression() {
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

    public boolean isSupportsWritingToStdout()
    {
	return false;
    }

    public void setWritingToStdout(boolean b)
    {
	
    }
    
    public boolean getWritingToStdout()
    {
	return false;
    }

    /**
     * Get the <code>CompoundFormat</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean getCompoundFormat() {
	return compoundFormat;
    }

    /**
     * Set the <code>CompoundFormat</code> value.
     *
     * @param newCompoundFormat The new CompoundFormat value.
     */
    public final void setCompoundFormat(final boolean newCompoundFormat) {
	this.compoundFormat = newCompoundFormat;
    }

    void luceneExtractProperties(MultiValueProp p)
    {
	super.luceneExtractProperties(p);
	
	if(p.containsKey(compoundFormatKey))
	    setCompoundFormat(Boolean.getBoolean(p.getProperty(compoundFormatKey).get(0)));
	
    }

    public String showState(int n)
    {
	StringBuilder sb = new StringBuilder();
	sb.append(ca.gnewton.lusql.util.Util.offset("DocSink: " + this.getClass().getName(),n) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("RAM buffer size: " + getRAMBufferSize(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("analyzer: " + getAnalyzerName(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("append to existing Lucene index? " + getAppendToSink(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("compound format: " + getCompoundFormat(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("create index? " + isIndexCreate(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("index dir name: " + getLuceneIndexName(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("is threaded: " + isThreaded(),n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("remove on done: " + isRemoveOnDone(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("requires primary key field: " + requiresPrimaryKeyField(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("stop word file: " + getStopWordFileName(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("supports compression: " + isSupportsCompression(), n+1) + "\n");
	sb.append(ca.gnewton.lusql.util.Util.offset("supports writing to stdout: " + isSupportsWritingToStdout(), n+1) + "\n");



	return sb.toString();
    }

    public boolean isThreadSafe()
    {
	return true;
    }

    boolean threadSafe = false;
    
    public void setThreadSafe(final boolean newThreadSafe)
    {
	this.threadSafe = newThreadSafe;
    }
    

    boolean threaded = true;
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


}  //////




