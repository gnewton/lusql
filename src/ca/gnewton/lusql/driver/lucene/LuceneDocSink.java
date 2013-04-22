package ca.gnewton.lusql.driver.lucene;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;

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

public class LuceneDocSink 
	extends AbstractDocSink
	implements DocSink, ReaderFieldDocSink
{
	final static String compoundFormatKey = "compoundFormat";
	static
	{
		System.setProperty("org.apache.lucene.FSDirectory.class",NIOFSDirectory.class.getName());
	}

	private String primaryKeyField = null;
	static private LuceneCore luceneCore;
	
	static
	{
		initLucenCore();
	}

	static void initLucenCore()
	{
		luceneCore = new LuceneCore();
	}
	
	
	
	public void setPrimaryKeyField(String primaryKeyField)
	{
		this.primaryKeyField = primaryKeyField;
	}

	private boolean removeOnDone = false;;
	
	public void setRemoveOnDone(boolean removeOnDone)
	{
		this.removeOnDone = removeOnDone;
	}
	
    
	//static Category cat = Category.getInstance(LuSql.class.getName());
	public boolean requiresPrimaryKeyField()
	{
		return false;
	}

	public String description()
	{
		return "Sink that indexes Documents into Lucene";
	}

	private boolean supportsCompression = false;

	private boolean compoundFormat = true;

	public LuceneDocSink() {

	}

	private int addDocHintSize = 1;
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

	IndexWriter writer = null;
	public void init(MultiValueProp p) 
		throws PluginException
	{
		try
			{
				extractProperties(p);
				if(p.containsKey(LuceneDocSink.compoundFormatKey))
					setCompoundFormat(Boolean.parseBoolean(p.getProperty(LuceneDocSink.compoundFormatKey).get(0)));
		
				Analyzer ana = luceneCore.newAnalyzer();
				if(ana == null)
					throw new PluginException("Problem with Analyzer: null;  Should be: "
					                          + luceneCore.getAnalyzerName());

				/*
				  writer = new IndexWriter(makeDirectory(),
				  ana,
				  isIndexCreate(),
				  maxFieldLength
				  // fixxx 2010.07.08
				  );
				*/
				IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, ana).setRAMBufferSizeMB(luceneCore.getRAMBufferSize());
		
		
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
	
		return FSDirectory.open(new File(luceneCore.getLuceneIndexName()));

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
						System.err.println("Exception on " + luceneCore.getLuceneIndexName());
						t.printStackTrace();
						throw new DocSinkException("Failed addDocument");
					}
			}
	}

	public void done()  
		throws PluginException
	{
		if(writer != null){
			try{
				writer.commit();
			}
			catch(Throwable t){
				t.printStackTrace();
			}
			try{
				writer.close();
			}
			catch(Throwable t){
				t.printStackTrace();
			}
		}
		
		//TODO removeOnDone
		if(isRemoveOnDone())
			{
				//cat.info("Removing Lucene index directory: " + getLuceneIndexName());
				ca.gnewton.lusql.core.Util.removeDir(luceneCore.getLuceneIndexName());
			}
	}

	private Document makeDocument(Doc doc)
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
		return writer;
	}

	void extractProperties(MultiValueProp p)
	{
		luceneExtractProperties(p);
		if(p.containsKey(LuSqlFields.RemoveSinksOnDoneKey)){
			setRemoveOnDone(Boolean.parseBoolean(p.getProperty(LuSqlFields.RemoveSinksOnDoneKey).get(0)));
		}
		
	}

	public String descriptor()
	{
		return this.getClass().getName() 
			+ (
			   (luceneCore.getLuceneIndexName()==null)
			   ?""
			   :("Writing to location: " + luceneCore.getLuceneIndexName())
			   )
			;
	} 

	public final boolean isSupportsCompression() {
		return supportsCompression;
	}

	public final void setSupportsCompression(final boolean newSupportsCompression) {
		this.supportsCompression = newSupportsCompression;
	}

	public void setWritingToStdout(boolean b)
	{
	
	}

	
	public boolean getWritingToStdout()
	{
		return false;
	}

	public final boolean getCompoundFormat() {
		return compoundFormat;
	}

	public final void setCompoundFormat(final boolean newCompoundFormat) {
		this.compoundFormat = newCompoundFormat;
	}

	void luceneExtractProperties(MultiValueProp p)
	{
		luceneCore.luceneExtractProperties(p);
	
		if(p.containsKey(compoundFormatKey))
			setCompoundFormat(Boolean.getBoolean(p.getProperty(compoundFormatKey).get(0)));
	
	}

	public String showState(int n)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(ca.gnewton.lusql.util.Util.offset("DocSink: " + this.getClass().getName(),n) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("RAM buffer size: " + luceneCore.getRAMBufferSize(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("analyzer: " + luceneCore.getAnalyzerName(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("append to existing Lucene index? " + luceneCore.getAppendToSink(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("compound format: " + getCompoundFormat(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("create index? " + luceneCore.isIndexCreate(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("index dir name: " + luceneCore.getLuceneIndexName(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("is threaded: " + luceneCore.isThreaded(),n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("remove on done: " + luceneCore.isRemoveOnDone(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("requires primary key field: " + requiresPrimaryKeyField(), n+1) + "\n");
		sb.append(ca.gnewton.lusql.util.Util.offset("stop word file: " + luceneCore.getStopWordFileName(), n+1) + "\n");
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

	public final boolean isThreaded() {
		return threaded;
	}

	public final void setThreaded(final boolean newThreaded) {
		this.threaded = newThreaded;
	}    


}  //////




