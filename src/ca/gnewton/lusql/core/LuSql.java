package ca.gnewton.lusql.core;

/*
 * @copyright CISTI / National Research Council Canada and Glen Newton
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */

import ca.gnewton.lusql.util.Util;
import ca.gnewton.lusql.util.AnnotationUtil;
import ca.gnewton.lusql.util.LoadAvg;
import ca.gnewton.lusql.util.MultiValueProp;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;

import java.lang.annotation.*;
import java.lang.reflect.Constructor;

import java.sql.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.*;
import java.io.InputStreamReader;

import javax.sql.DataSource;

import org.apache.commons.cli.*;
import org.apache.commons.dbcp.*;
import org.apache.commons.dbcp.cpdsadapter.*;
import org.apache.commons.dbcp.datasources.*;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;

public class LuSql
	implements LuceneFields, LuSqlFields
{
	public static final double ThreadFactor = 1.5;
	public static float loadAverageLimit = 7.0f;

	private InputStream inputStream = null;
	
	int docPacketSize = DefaultDocPacketSize;    
     
	public static LuceneFieldParameters defaultLuceneFieldParameters;

	{
		defaultLuceneFieldParameters = new LuceneFieldParameters(Field.Index.ANALYZED,
		                                                         Field.Store.YES,
		                                                         Field.TermVector.NO);
	}

	public static Version luceneVersion = Version.LUCENE_36;

	//static Category cat = Category.getInstance(LuSql.class.getName());
	boolean docPoolFlag = false;
	boolean addDocPoolFlag = false;

	public List<String> secondaryIndexNames = new ArrayList<String>(1);
	public List<String> recordFields = null;
	public List<SubQuery> subQueries = null;
	public Doc globalFields = null;

	public static boolean debug = false;

	private String query = null;
	private String AnalyzerName = DefaultAnalyzerClassName;
	private String StopWordFileName = null;
	private String DBDriverName = DefaultJDBCDriverClassName;
	private String DBUrl = null;

	private Map<String, LuceneFieldParameters> fieldIndexParameters2 = new HashMap<String, LuceneFieldParameters>();

	private int transactionIsolation = DefaultTransactionIsolation;
	private String SinkLocationName = DefaultSinkLocationName;
	private boolean appendToLuceneIndex;

	private boolean MySql = true;

	private boolean Test = false;
	private boolean usingRecordFields = false;


	private DocFilter preIndexDocFilter = null;


	private List<String> docFilterNames = new ArrayList<String>();;


	LuceneFieldParameters globalFieldIndexParameter = null;
	private long maxDocs = Long.MAX_VALUE;

	private boolean indexCreate = true;

	private int chunkSize = 0;


	public static boolean verbose;


	private boolean threaded = true;


	private double RAMBufferSizeMB = DefaultRAMBufferSizeMB;


	private MultiValueProp properties;

	private String propertiesFileName;

	private String[] args;

	private int numThreads = (int)((float)(Runtime.getRuntime().availableProcessors()) *ThreadFactor);

	private boolean fatalError = false;


	private boolean merge = true;


	private String docSourceClassName=DefaultDocSourceClassName;
	private String docSinkClassName=DefaultDocSinkClassName;
	private int offset = LuSqlFields.OffsetDefault;

	private String primaryKeyField;
	private String docSourceFile;
	private int transactionLevel;
	private int queueSize = -1;

	public LuSql() 
	{
		subQueries = new ArrayList<SubQuery>(1);
	}



	void setRecordFields(final List<String>newRecordFields)
	{
		recordFields = newRecordFields;
	}

	List<String> getRecordFields()
	{
		return recordFields;
	}
	/**
	 * Get the <code>Query</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getQuery() {
		return query;
	}

	/**
	 * Set the <code>Query</code> value.
	 *
	 * @param newQuery The new Query value.
	 */
	public final void setQuery(final String newQuery) {
		this.query = newQuery;
	}

	/**
	 * Get the <code>AnalyzerName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getAnalyzerName() {
		return AnalyzerName;
	}

	/**
	 * Set the <code>AnalyzerName</code> value.
	 *
	 * @param newAnalyzerName The new AnalyzerName value.
	 */
	public final void setAnalyzerName(final String newAnalyzerName) {
		this.AnalyzerName = newAnalyzerName;
	}

	/**
	 * Get the <code>StopWordFileName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getStopWordFileName() {
		return StopWordFileName;
	}

	/**
	 * Set the <code>StopWordFileName</code> value.
	 *
	 * @param newStopWordFileName The new StopWordFileName value.
	 */
	public final void setStopWordFileName(final String newStopWordFileName) {
		this.StopWordFileName = newStopWordFileName;
	}

	/**
	 * Get the <code>DBDriverName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getDBDriverName() {
		return DBDriverName;
	}

	/**
	 * Set the <code>DBDriverName</code> value.
	 *
	 * @param newDBDriverName The new DBDriverName value.
	 */
	public final void setDBDriverName(final String newDBDriverName) {
		this.DBDriverName = newDBDriverName;
	}

	/**
	 * Get the <code>DBUrl</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getDBUrl() {
		return DBUrl;
	}

	/**
	 * Set the <code>DBUrl</code> value.
	 *
	 * @param newDBUrl The new DBUrl value.
	 */
	public final void setDBUrl(final String newDBUrl) {
		this.DBUrl = newDBUrl;
	}



	/**
	 * Get the <code>SinkLocationName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getSinkLocationName() {
		return SinkLocationName;
	}

	/**
	 * Set the <code>SinkLocationName</code> value.
	 *
	 * @param newSinkLocationName The new SinkLocationName value.
	 */
	public final void setSinkLocationName(final String newSinkLocationName) {
		this.SinkLocationName = newSinkLocationName;
	}

	/**
	 * Get the <code>AppendToLuceneIndex</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isAppendToLuceneIndex() {
		return appendToLuceneIndex;
	}

	/**
	 * Set the <code>AppendToLuceneIndex</code> value.
	 *
	 * @param newAppendToLuceneIndex The new AppendToLuceneIndex value.
	 */
	public final void setAppendToLuceneIndex(final boolean newAppendToLuceneIndex) {
		this.appendToLuceneIndex = newAppendToLuceneIndex;
	}


	/**
	 * Get the <code>MySql</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isMySql() {
		return MySql;
	}

	/**
	 * Set the <code>MySql</code> value.
	 *
	 * @param newMySql The new MySql value.
	 */
	public final void setMySql(final boolean newMySql) {
		this.MySql = newMySql;
	}

	/**
	 * Get the <code>Test</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isTest() {
		return Test;
	}




	/**
	 * Set the <code>Test</code> value.
	 *
	 * @param newTest The new Test value.
	 */
	public final void setTest(final boolean newTest) {
		this.Test = newTest;
	}
	static GenericObjectPool docPool = null;
	static GenericObjectPool addPool = null;
	static DocFactory df = null;
	int recordCount = 0;

	long d0;

	public void init()
	{
		try
			{
				initPools();
				if(globalFieldIndexParameter == null)
					globalFieldIndexParameter = new LuceneFieldParameters();
		 
				DocSource docSource = makeDocSource();
				initDocSource(docSource);
				setDocSource(docSource);

				initFilterProperties();
				// fix
				initFilter(docSource);
				initSubQueryJoinFilters(docSource);
				initMappingFilter();

				AddDocument.setSubQueryJoinFilters(subQueryJoinFilters);
		
				initThreadPoolExecutor();
				if(verbose)
					System.err.println(this.getClass().getName());

				setDocSink(makeDocSink(getSinkProperties()));		
				AddDocument.setDocSink(getDocSink());
				getDocSink().setPrimaryKeyField(getPrimaryKeyField());
			}
		catch(Throwable t)
			{
				t.printStackTrace();
				throw new NullPointerException();
			}
	 
	 

	}
     

	public void run()
	{
		d0 = System.currentTimeMillis();	    
		try
			{
	
				try
					{
						if(docSource.isThreadSafe())
							{
								parallelSource(docSource);
							}
						else
							serialSource(docSource);
					}
				catch(DataSourceException t)
					{
						t.printStackTrace();
					}
				finally
					{		
						docSource.done();
						done();
						docSink.commit();
						docSink.done();
						if(verbose)
							{
								//cat.info("Shutting down filters");
							}
						shutDownFilters();
		
						if(verbose)
							{
								//cat.info("Shutting down subqueries");
							}
						onDoneSubQueryJoinFilters();
					}
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}
	}


	void parallelSource(DocSource docSource)
		throws DataSourceException
	{
		serialSource(docSource);
	}

	void serialSource(DocSource docSource)
		throws DataSourceException
	{
		Doc doc = null;
		int count = 0;
		int added = 0;
		boolean done = false;
		int docCount = 0;
		int docChunksCount = 0;

		Doc[] docs = new Doc[docPacketSize];
		int n = 0;
	    
		while(count < maxDocs && !fatalError && !done)
			{
				doc = docSource.next();
				if(doc == null)
					continue;
		    
				if(doc.isLast()){
					System.out.println("LuSql: islast");
					break;
				}
				
				docs[n] = doc;
				n++;
				count++;
				if(n >= docPacketSize)
					{
						docCount += docs.length;
						
						addDoc(docs);
						++docChunksCount;
						
						docs = new Doc[docPacketSize];
						n = 0;
						feedbackOutput(count);
						checkLoad();
					}
			}
		// flush out the last
		System.out.println("LuSql: flushing out the remainder: " + n);

		Doc[] endDocs = new Doc[n];
		for(int i=0; i<n; i++)
			{
				endDocs[i] = docs[i];
			}
		addDoc(endDocs);
		++docChunksCount;
		docCount += endDocs.length;

		while(threadPoolExecutor.getQueue().size() > 0 && threadPoolExecutor.getActiveCount() > 0){
			try{
				Thread.currentThread().sleep(500);
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
			System.out.print("@");
		}
	
		endDocs = new Doc[1];
		Doc lastDoc = new DocImpLast();
		lastDoc.setLast(true);
		((DocImpLast)lastDoc).totalCount = docCount;
		
		endDocs[0] = lastDoc;
		addDoc(endDocs);
		++docChunksCount;

		System.out.println("LuSql: Total docs sent: " + docCount);
		System.out.println("LuSql: Total chunks doc sent: " + docChunksCount);
	}
     


	void initMappingFilter()
	{
		if(fieldMap == null || fieldMap.size() == 0)
			return;
		FieldMapFilter fmf = new FieldMapFilter();
		fmf.setFieldMap(fieldMap);
		fmf.setOnlyMap(isOnlyMap());
		((LinkedList<DocFilter>)docFilters).addFirst(fmf);
	    
	}

	void shutDownFilters()
		throws PluginException
	{
		if(getDocFilters() == null)
			return;
		for(DocFilter f: getDocFilters())
			if(f != null)
				f.done();
	}


	void done()
	{
		try
			{
				//flush();
				if(threadPoolExecutor != null)
					{
						long time0 = System.nanoTime();
						if(verbose)
							{
								//cat.info("Shutting down executor");
							}
						threadPoolExecutor.shutdown();
						threadPoolExecutor.awaitTermination(1000l,
						                                    TimeUnit.SECONDS);

						if(verbose)
							{
								//cat.info("Time to index: " + (System.currentTimeMillis() - d0)/1000 + "s");
							}
						long timeToIndex = System.nanoTime() - time0;
						if(verbose)
							{
								//cat.info("Shut down executor: t=" + ((double)timeToIndex)/1000000000d + "s   ");
							}
					}
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}
	}

	void flush()
	{
		/*
		  if(docList.size() > 0)
		  {
		  AddDocument ad = new AddDocument();
		  ad.setDocSink(docSink);
		  ad.setDocs(docs);
		  ad.setFilters(getDocFilters());
		  ad.setLuSql(this);
		  threadPoolExecutor.execute(ad);
		  }
		*/
	}


	long t0 = System.currentTimeMillis();


	void feedbackOutput(final int count)
	{
	    
		if(!verbose)
			return;
		
		if(outputChunk != 0){
			if(count%(outputChunk/10) == 0)
				{
					System.err.print(".");
					//System.out.println(count + getSystemInfo());
				}
		}
		
		if(count%outputChunk == 0)
			{
				System.err.println(count + "    " + timeDelta() + "s"
				                   /*+ "        ABSTIME:" 
				                     + count
				                     + ":" 
				                     + System.currentTimeMillis()/1000
				                     + getSystemInfo()*/
				                   );
				t0 = System.currentTimeMillis();
			} 
	}
    
	private volatile AtomicLong realCount = new AtomicLong(0);

	private final ReentrantLock lock = new ReentrantLock();
	private boolean onlyMap = false;
	private DocSink docSink;
	private boolean sourceCompression = false;
	private boolean sinkCompression = false;
	private MultiValueProp sourceProperties;
	private MultiValueProp sinkProperties;
	private DocSource docSource;

	private int outputChunk = DefaultChunkSize;
	private int workPerThread;
	private String userid = null;
	private String password = null;
	private boolean sinkWriteToStdout = false;
	private boolean sinkReadFromStdin;

	private Set<String> fieldNames = new HashSet<String>();
	private Map<String,String> fieldMap;

	public void incrementRealCount()
	{
		realCount.incrementAndGet();
	}

	void initFilter(DocSource docSource)
		throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, 	
		       java.lang.reflect.InvocationTargetException
	{
		setDocFilters(instantiateDocFilters());
		AddDocument.setFilters(getDocFilters());
		if(getDocFilters() == null)
			return;
		int n = 0;
		for(DocFilter df: getDocFilters())
			{
				if(df != null)
					{
						if(df instanceof DBDocFilter)
							// FIXX
							((DBDocFilter)df).setDataSource(((ca.gnewton.lusql.driver.jdbc.JDBCDocSource)docSource).getDataSource());
					}
				try
					{
						df.init(filterProperties.get(""+n));
						System.out.println("LuSql: filter properties:" 
						                   + filterProperties.get(""+n));
					}
				catch(Throwable t)
					{
						t.printStackTrace();
						throw new NullPointerException();
					}
				++n;
			}
	}

	float timeDelta()
	{
		long t1 = System.currentTimeMillis() - t0;
		return (float)(t1/100l)/10f;
	}

    

	List<Doc> docList = new ArrayList<Doc>(workPerThread);

	public void addDoc(Doc[] docs)
	{
		if(docSink == null)
			throw new NullPointerException("DocSink is null");
		for(int i=0; i<docs.length; i++)
			{
				if(docs[i] != null && !docs[i].isLast())
					{
						initDocParameters(docs[i]);
						docs[i].add(globalFields);
					}
		    
			}
		AddDocument ad = new AddDocument();
		ad.setDocs(docs);
		if(docSink == null)
			throw new NullPointerException("DocSink is null");
		AddDocument.setFilters(getDocFilters());
		ad.setLuSql(this);
		//ca.gnewton.lusql.util.LoadInfo.loadAvgLimiter(loadAverageLimit, 30, true);
		//System.err.print("*");
	    
		threadPoolExecutor.execute(ad);
	}
     


	void initDocParameters(Doc doc)
	{
		Iterator<String> it = doc.getFieldNames();
		while(it.hasNext())
			{
				String fieldName = it.next();
				doc.addFieldParameter(fieldName, getLuceneFieldParameters(fieldName));
			}
	}


	public int makeQueueSize()
	{
		if(queueSize == -1)
			return numThreads * 3;
		else
			return queueSize;
	}

	void initThreadPoolExecutor()
	{
		if(numThreads < minThreadPoolThreads)
			minThreadPoolThreads = numThreads;

	 
		recordQueue = new ArrayBlockingQueue<Runnable>(makeQueueSize());
		//threadPoolExecutor = new ThreadPoolExecutor(2,
		if(threadPoolExecutor == null)
			threadPoolExecutor = 
				new AddDocumentExecutor(minThreadPoolThreads,
				                        numThreads,
				                        16l,
				                        TimeUnit.SECONDS,
				                        recordQueue,
				                        new ThreadPoolExecutor.CallerRunsPolicy(),
				                        this);
	}


	

	public static final DocSource instantiateDocSource(String sourceClassName)
		throws PluginException
	{
		return (DocSource)(Util.instantiateClass(sourceClassName));
	}

	public static final DocSink instantiateDocSink(String sinkClassName)
		throws PluginException
	{
		return (DocSink)(Util.instantiateClass(sinkClassName));
	}
	

	DocSource makeDocSource()
		throws ClassNotFoundException,
		       NoSuchMethodException,
		       InstantiationException,
		       IllegalAccessException,
		       PluginException,
		       java.lang.reflect.InvocationTargetException
	{
		String className = getDocSourceClassName();
		
		DocSource source = instantiateDocSource(className);

		Map<String, MultiValueProp> allPluginProps = new HashMap<String, MultiValueProp> ();
		MultiValueProp mvp = new MultiValueProp();
		mvp.put("numDocs", "100");
		allPluginProps.put("ca.gnewton.lusql.driver.faux.IntegerDocumentDocSource",
		                   mvp);

		Class<? extends DocSource> docSourceClass = source.getClass();

		//Annotation a = docSourceClass.getAnnotation(PluginParameter.class) ;

		AnnotationUtil.handleAnnotations(docSourceClass);
		

		return source;
	    
	}


     

	DocSink makeDocSink(MultiValueProp p)
		throws ClassNotFoundException,
		       NoSuchMethodException,
		       InstantiationException,
		       IllegalAccessException,
		       java.lang.reflect.InvocationTargetException,
		       PluginException
	{

		DocSink docSink = makeDocSink(getDocSinkClassName(), this.isSinkWriteToStdout());

		// FIXXX
		p.setProperty(AnalyzerClassKey, getAnalyzerName());
		p.setProperty(SinkLocationKey, getSinkLocationName());
		p.setUnique(SinkLocationKey);
		p.setProperty(CreateSinkKey,  Boolean.toString(isIndexCreate()));
		p.setProperty(BufferSizeKey,  Double.toString(getRAMBufferSizeMB()));
	    
		if(getStopWordFileName() != null)
			p.setProperty(StopWordFileNameKey,  getStopWordFileName());

		if(secondaryIndexNames != null)
			for(int i=0; i<secondaryIndexNames.size(); i++)
				{
					p.setProperty(SecondarySinkLocationKey + i, 
					              secondaryIndexNames.get(i));
					System.out.println("---- > SecondaryIndex: " 
					                   + SecondarySinkLocationKey + i
					                   + ":" 
					                   +secondaryIndexNames.get(i)
					                   );
				}
	 
		docSink.init(p);
		return docSink;
	}
     


	static DocSink makeDocSink(String sinkClassName, boolean isSinkWriteToStdout)
		throws ClassNotFoundException,
		       NoSuchMethodException,
		       InstantiationException,
		       IllegalAccessException,
		       java.lang.reflect.InvocationTargetException,
		       PluginException
	{
		DocSink docSink = instantiateDocSink(sinkClassName);
		Class<?> docSinkClass = docSink.getClass();

		if(docSink.isSupportsWritingToStdout() && !isSinkWriteToStdout)
			throw new PluginException("Sink does not support write to stdout");

		if(isSinkWriteToStdout)
			docSink.setWritingToStdout(true);

		return docSink;

	}


	void initPools()
	{
		if(docPoolFlag)
			{
				df = new DocFactory();
				docPool = new GenericObjectPool(df);
				docPool.setLifo(true);
				docPool.setMaxActive(-1);
				docPool.setMaxIdle(1000);
			}
	    
		if(addDocPoolFlag)
			{
				// AddDocument pool
				AddDocumentFactory adf = new AddDocumentFactory();
				addPool = new GenericObjectPool(adf);
				addPool.setLifo(true);
				addPool.setMaxActive(-1);
				addPool.setMaxIdle(8);
			}

	}

 
	/* Instantiate the analyzer using reflection */
	List<DocFilter> instantiateDocFilters()
		throws ClassNotFoundException,
		       NoSuchMethodException,
		       InstantiationException,
		       IllegalAccessException,
		       java.lang.reflect.InvocationTargetException
	{
		List<DocFilter> newFilters = new LinkedList<DocFilter>();
		if(getDocFilterNames() == null
		   || getDocFilterNames().size() == 0)
			return newFilters;


		for(String filterName: getDocFilterNames())
			{
				if(LuSql.verbose)
					{
						//cat.info("filterName=" + filterName);
					}
				Class<?> filterClass = Class.forName(filterName);
		
				Constructor<? extends Object> constructor = filterClass.getConstructor();
				newFilters.add((DocFilter)constructor.newInstance());
			}
		return newFilters;
	}



	ResultSetMetaData md = null;
	int nFields;

	LuceneFieldParameters[] paras;

	    


	/**
	 * Get the <code>UsingRecordFields</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isUsingRecordFields() {
		return usingRecordFields;
	}

	/**
	 * Set the <code>UsingRecordFields</code> value.
	 *
	 * @param newUsingRecordFields The new UsingRecordFields value.
	 */
	public final void setUsingRecordFields(final boolean newUsingRecordFields) {
		this.usingRecordFields = newUsingRecordFields;
	}

  
  		
	String[] lastFieldValues = null;
    
	// the document that accumulates the content
	Document accDoc = null;
 
  

	void resetPreviousValues(ResultSet rs, String[] last, List<String> recordFields)
		throws SQLException
	{
		for(int j=0; j<lastFieldValues.length; j++)
			{
				last[j] = rs.getString(recordFields.get(j));
			}
	}


    
	boolean isJoinField(String fieldName)
	{
		return recordFields.contains(fieldName);
	}

	static public Doc newDoc()
	{
		return new DocImp();
		/*
		  try
		  {
		  return (Doc)docPool.borrowObject();
		  }
		  catch(Throwable t)
		  {
		  t.printStackTrace();
		  System.err.println("Problem with Doc pool: reverting to public Doc constructor");
		  return new DocImp();
		  }
		*/
	}


	BlockingQueue<Runnable> recordQueue = null;
	AddDocumentExecutor threadPoolExecutor = null;



	int minThreadPoolThreads = 2;
	 
	List<DocFilter> docFilters = new LinkedList<DocFilter>();
	/**
	 * Get the <code>DocumentFilter</code> value.
	 *
	 * @return a <code>DocumentFilter</code> value
	 */
	public final List<DocFilter> getDocFilters() {
		return docFilters;
	}

	/**
	 * Set the <code>DocumentFilter</code> value.
	 *
	 * @param newocumentFilter The new DocumentFilter value.
	 */
	public final void setDocFilters(final List<DocFilter> newDocFilters) {
		this.docFilters = newDocFilters;
	}

	/**
	 * Get the <code>DocumentFilterName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final List<String> getDocFilterNames() {
		return docFilterNames;
	}

	/**
	 * Set the <code>DocFilterName</code> value.
	 *
	 * @param newDocFilterName The new DocumentFilterName value.
	 */
	public final void setDocFilterNames(final List<String> newDocFilterNames) {
		this.docFilterNames = newDocFilterNames;
	}

	void printResultSet(ResultSet rs, int n)
		throws java.sql.SQLException
	{
		/*
		  StringBuilder sb = new StringBuilder();
		  for(int i=1; i<=n; i++)
		  sb.append(rs.getString(i) + " ");

		  System.out.println(sb);
		*/
	    
		System.out.println(rs.getString(1));
	}

	/**
	 * Get the <code>MaxDocs</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final long getMaxDocs() {
		return maxDocs;
	}

	/**
	 * Set the <code>MaxDocs</code> value.
	 *
	 * @param newMaxDocs The new MaxDocs value.
	 */
	public final void setMaxDocs(final long newMaxDocs) {
		this.maxDocs = newMaxDocs;
	}

	/**
	 * Get the <code>IndexCreate</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isIndexCreate() {
		return indexCreate;
	}

	/**
	 * Set the <code>IndexCreate</code> value.
	 *
	 * @param newIndexCreate The new IndexCreate value.
	 */
	public final void setIndexCreate(final boolean newIndexCreate) {
		this.indexCreate = newIndexCreate;
	}

	/**
	 * Get the <code>ChunkSize</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getChunkSize() {
		return chunkSize;
	}

	/**
	 * Set the <code>ChunkSize</code> value.
	 *
	 * @param newChunkSize The new ChunkSize value.
	 */
	public final void setChunkSize(final int newChunkSize) {
		this.chunkSize = newChunkSize;
	}

	/**
	 * Get the <code>Verbose</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	static public final boolean isVerbose() {
		return verbose;
	}

	/**
	 * Set the <code>Verbose</code> value.
	 *
	 * @param newVerbose The new Verbose value.
	 */
	static public final void setVerbose(final boolean newVerbose) {
		verbose = newVerbose;
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

	/**
	 * Get the <code>RAMBufferSizeMB</code> value.
	 *
	 * @return a <code>double</code> value
	 */
	public final double getRAMBufferSizeMB() {
		return RAMBufferSizeMB;
	}

	/**
	 * Set the <code>RAMBufferSizeMB</code> value.
	 *
	 * @param newRAMBufferSizeMB The new RAMBufferSizeMB value.
	 */
	public final void setRAMBufferSizeMB(final double newRAMBufferSizeMB) {
		this.RAMBufferSizeMB = newRAMBufferSizeMB;
	}

	/**
	 * Get the <code>Properties</code> value.
	 *
	 * @return a <code>Properties</code> value
	 */
	public final MultiValueProp getProperties() {
		if(properties == null)
			properties = new MultiValueProp();
	
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
	 * Get the <code>PropertiesFileName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getPropertiesFileName() {
		return propertiesFileName;
	}

	/**
	 * Set the <code>PropertiesFileName</code> value.
	 *
	 * @param newPropertiesFileName The new PropertiesFileName value.
	 */
	public final void setPropertiesFileName(final String newPropertiesFileName) {
		this.propertiesFileName = newPropertiesFileName;
	}


	int dcount = 0;
	public void returnDoc(Doc doc)
	{
		if(!docPoolFlag)
			return;

		if(dcount%500 == 0)
			{

				//System.out.println("*DocumentFactory idle=" + pool.getNumIdle());
				//System.out.println("*DocumentFactory active=" + pool.getNumActive());		
			}	    
		try
			{
				docPool.returnObject(doc);

			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}

	}

	int addCount = 0;
	public void returnAddDocument(AddDocument ad)
	{
		if(!addDocPoolFlag)
			return;

		++addCount;
		if(addCount %1000 == 0)
			{
				//System.out.println("AddDocumentFactory idle=" + addPool.getNumIdle());
				//System.out.println("AddDocumentFactory active=" + addPool.getNumActive());
			}
		try
			{
				addPool.returnObject(ad);

			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}

	}


	/**
	 * Get the <code>Args</code> value.
	 *
	 * @return a <code>String[]</code> value
	 */
	public final String[] getArgs() {
		return args;
	}

	/**
	 * Set the <code>Args</code> value.
	 *
	 * @param newArgs The new Args value.
	 */
	public final void setArgs(final String[] newArgs) {
		this.args = newArgs;
	}

	void printResultSet(ResultSet rs, String fieldNames[])
		throws SQLException
	{
		int nFields = fieldNames.length;
		StringBuilder s = new StringBuilder("> ");
		for(int i=1; i<=nFields; i++)
			{
				String f = rs.getString(i);
				s.append(fieldNames[i-1]);
				s.append("=");
		
				if(f != null)
					{
						f = f.trim();
						if(f.length() > 18)
							f = f.substring(0,18).trim() + "...";
					}
				s.append(f);
				s.append("; ");
			}
		System.out.println(s);
	}

	public List<SubQuery>getSubQueries()
	{
		return subQueries;
	}

	public void addSubQuery(final SubQuery newSubQuery)
	{
		subQueries.add(newSubQuery);
	}

	public void addSecondaryIndexName(final String newSecondaryIndexName)
	{
		secondaryIndexNames.add(newSecondaryIndexName);
	}

	SubQueryJoinFilter[] subQueryJoinFilters = null;
	void initSubQueryJoinFilters(DocSource docSource)
	{
		if(subQueries == null ||  subQueries.size() == 0)
			return;

		// FIXX??
		if(!(docSource instanceof ca.gnewton.lusql.driver.jdbc.JDBCDocSource))
			{
				System.err.println("Error: DocSource is not JDBC; unable to have subqueries");
				return;
			}

		int n = subQueries.size();
		subQueryJoinFilters = new SubQueryJoinFilter[n];
		for(int i=0; i<n; i++)
			{
				subQueryJoinFilters[i] = new SubQueryJoinFilter(subQueries.get(i));
				subQueryJoinFilters[i].setDataSource(((ca.gnewton.lusql.driver.jdbc.JDBCDocSource)docSource).getDataSource());
				subQueryJoinFilters[i].setProperties(filterProperties.get(""+ i));
				if(subQueryJoinFilters[i].getProperties() == null)
					subQueryJoinFilters[i].setProperties(new MultiValueProp());
				subQueryJoinFilters[i].getProperties().setProperty(LuSqlFields.IsMysqlKey, Boolean.toString(isMySql()));	    
				subQueryJoinFilters[i].setLuSql(this);
			}
	    
	}

	void onDoneSubQueryJoinFilters()
	{
		if(subQueries == null ||  subQueries.size() == 0)
			return;

		int n = subQueries.size();
		for(int i=0; i<n; i++)
			{
				subQueryJoinFilters[i].onDone();
			}
	    
	}

	void initFilterProperties()
	{		    
		MultiValueProp p = getProperties();
		if(p == null)
			p = new MultiValueProp();

		p.setProperty(LuSqlFields.JDBCDriverClassKey, getDBDriverName());
		p.setProperty(LuSqlFields.AnalyzerClassKey, getAnalyzerName());
		if(getStopWordFileName() != null)
			p.setProperty(LuSqlFields.StopWordFileNameKey, getStopWordFileName());
		if(getDBUrl() != null)
			p.setProperty(LuSqlFields.DBUrlKey, getDBUrl());
		p.setProperty(LuSqlFields.SinkLocationKey, getSinkLocationName());
		p.setProperty(LuSqlFields.IsMysqlKey, Boolean.toString(isMySql()));	    
	    
		/*
		  Iterator<String> it = filterProperties.keySet().iterator();
		  while(it.hasNext())
		  {
		  Properties fp = filterProperties.get(it.next());
		  fp.putAll(p);
		  }
		*/


		if(isMySql())
			p.setProperty(JDBCFetchSizeKey, "0");
		else
			p.setProperty(JDBCFetchSizeKey, "100");
	}

	/*
	  Document runSubQueries(Document document)
	  {
	  if(subQueryJoinFilters != null && false)
	  {
	  for(int i=0; i<subQueryJoinFilters.length; i++)
	  {
	  document = subQueryJoinFilters[i].filter(document);
	  }
	  }
	  return document;
	  }
	*/

	/**
	 * Get the <code>NumThreads</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getNumThreads() {
		return numThreads;
	}

	/**
	 * Set the <code>NumThreads</code> value.
	 *
	 * @param newNumThreads The new NumThreads value.
	 */
	public final void setNumThreads(final int newNumThreads) {
		this.numThreads = newNumThreads;
	}

	/**
	 * Get the <code>FatalError</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isFatalError() {
		return fatalError;
	}

	/**
	 * Set the <code>FatalError</code> value.
	 *
	 * @param newFatalError The new FatalError value.
	 */
	public final void setFatalError(final boolean newFatalError) {
		this.fatalError = newFatalError;
	}



	long verboseRecordOutput(int recordCount, boolean first, long t0)
	{
		if((recordCount)%(outputChunk/10) == 0 && recordCount > 1)
			//if(first)
			System.out.print(".");
		//else
		//System.out.print(",");
		if((recordCount)%outputChunk== 0 && recordCount > 1)
			{
				System.out.println(" " + recordCount + " docs    " + (System.currentTimeMillis()-t0)/1000 + "s");
				System.out.println(outputChunk);
				
				t0 = System.currentTimeMillis();
			}
		return t0;
	}

	/**
	 * Get the <code>Merge</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isMerge() {
		return merge;
	}

	/**
	 * Set the <code>Merge</code> value.
	 *
	 * @param newMerge The new Merge value.
	 */
	public final void setMerge(final boolean newMerge) {
		this.merge = newMerge;
	}




	void addFieldIndexParameter(String field, String paras)
	{
		if(fieldIndexParameters2 == null)
			fieldIndexParameters2 = new HashMap<String, LuceneFieldParameters>();
		if(LuSql.verbose)
			{
				//cat.info("addFieldIndexParameter: " + field);
			}
		fieldIndexParameters2.put(field, new LuceneFieldParameters(paras));
	}
    
	Map<String, LuceneFieldParameters> getFieldIndexParameters()
	{
		return fieldIndexParameters2;
	}

	/**
	 * Get the <code>DocSourceClassName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getDocSourceClassName() {
		return docSourceClassName;
	}

	/**
	 * Set the <code>DocSourceClassName</code> value.
	 *
	 * @param newDocSourceClassName The new DocSourceClassName value.
	 */
	public final void setDocSourceClassName(final String newDocSourceClassName) {
		this.docSourceClassName = newDocSourceClassName;
	}

	/**
	 * Get the <code>DocSinkClassName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getDocSinkClassName() {
		return docSinkClassName;
	}

	/**
	 * Set the <code>DocSinkClassName</code> value.
	 *
	 * @param newDocSinkClassName The new DocSinkClassName value.
	 */
	public final void setDocSinkClassName(final String newDocSinkClassName) {
		this.docSinkClassName = newDocSinkClassName;
	}


	/**
	 * Get the <code>PrimaryKeyField</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getPrimaryKeyField() {
		return primaryKeyField;
	}

	public final void setPrimaryKeyField(final String newPrimaryKeyField) {
		this.primaryKeyField = newPrimaryKeyField;
	}

	public final String getDocSourceFile() {
		return docSourceFile;
	}


	public final void setDocSourceFile(final String newDocSourceFile) {
		this.docSourceFile = newDocSourceFile;
	}

	synchronized LuceneFieldParameters getLuceneFieldParameters(String field)
	{
		if(fieldIndexParameters2.containsKey(field))
			return fieldIndexParameters2.get(field);
		else
			return globalFieldIndexParameter;
	}

	public final LuceneFieldParameters getGlobalFieldIndexParameter() {
		return globalFieldIndexParameter;
	}


	public final void setGlobalFieldIndexParameter(final LuceneFieldParameters globalFieldIndexParameter) {
		this.globalFieldIndexParameter = globalFieldIndexParameter;
	}

	public void addGlobalField(LuceneFieldParameters para, String fieldName, String value)
	{
		if(globalFields == null)
			globalFields = new DocImp();
		if(para == null)
			para = new LuceneFieldParameters();
		globalFields.addField(fieldName, value, para);
	}

	void initDocSource(final DocSource docSource)
		throws PluginException
	{
		if(fieldNames != null && fieldNames.size() > 0)
			{
				for(String f:fieldNames)
					docSource.addField(f);
			}
		System.out.println("LuSql: init docSource");
		docSource.init(makeDocSourceProperties());
		System.out.println("LuSql: init docSource DONE");
	}

	MultiValueProp makeDocSourceProperties()
	{
		MultiValueProp p = getSourceProperties();
		if(getQuery() != null)
			p.setProperty(QueryKey, getQuery());
		if(getDBUrl() != null)
			p.setProperty(DBUrlKey, getDBUrl());
		if(getDBDriverName() != null)
			p.setProperty(JDBCDriverKey, getDBDriverName());
		if(getDBUrl() != null)
			//p.setProperty("docSourceFile", getDocSourceFile());
			p.setProperty(LuSqlFields.SourceLocationKey, getDBUrl());
		if(isMySql())
			p.setProperty(JDBCFetchSizeKey, "0");
		else
			p.setProperty(JDBCFetchSizeKey, "100");
		return p;
	}


	/**
	 * Get the <code>Offset</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getOffset() {
		return offset;
	}

	/**
	 * Set the <code>Offset</code> value.
	 *
	 * @param newOffset The new Offset value.
	 */
	public final void setOffset(final int newOffset) {
		this.offset = newOffset;
	}

	/**
	 * Get the <code>TransactionLevel</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getTransactionLevel() {
		return transactionLevel;
	}

	/**
	 * Set the <code>TransactionLevel</code> value.
	 *
	 * @param newTransactionLevel The new TransactionLevel value.
	 */
	public final void setTransactionLevel(final int newTransactionLevel) {
		this.transactionLevel = newTransactionLevel;
	}

	/**
	 * Get the <code>TransactionIsolation</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getTransactionIsolation() {
		return transactionIsolation;
	}

	/**
	 * Set the <code>TransactionIsolation</code> value.
	 *
	 * @param newTransactionIsolation The new TransactionIsolation value.
	 */
	public final void setTransactionIsolation(final int newTransactionIsolation) {
		this.transactionIsolation = newTransactionIsolation;
	}

	String getSystemInfo()
	{

		Map<String, String>map = Proc.mem();
		if(map == null)
			return "[No proc info available]";

		StringBuilder sb = new StringBuilder();
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext())
			{
				String key = it.next();
				sb.append(" " + map.get(key)); 
			}
		sb.append("\n# ");
		it = map.keySet().iterator();
		int count = 1;
		while(it.hasNext())
			{
				String key = it.next();
				sb.append(" " + count + "::"  + key);
				++count;
			}

		return sb.toString();
	}

	/**
	 * Get the <code>QueueSize</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getQueueSize() {
		return queueSize;
	}

	/**
	 * Set the <code>QueueSize</code> value.
	 *
	 * @param newQueueSize The new QueueSize value.
	 */
	public final void setQueueSize(final int newQueueSize) {
		this.queueSize = newQueueSize;
	}

	/**
	 * Get the <code>FieldMap</code> value.
	 *
	 * @return a <code>Map<String,String></code> value
	 */
	public final Map<String,String> getFieldMap() {
		return fieldMap;
	}

	/**
	 * Set the <code>FieldMap</code> value.
	 *
	 * @param newFieldMap The new FieldMap value.
	 */
	public final void setFieldMap(final Map<String,String> newFieldMap) {
		this.fieldMap = newFieldMap;
	}

	/**
	 * Get the <code>OnlyMap</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isOnlyMap() {
		return onlyMap;
	}

	/**
	 * Set the <code>OnlyMap</code> value.
	 *
	 * @param newOnlyMap The new OnlyMap value.
	 */
	public final void setOnlyMap(final boolean newOnlyMap) {
		this.onlyMap = newOnlyMap;
	}

	/**
	 * Get the <code>FieldNames</code> value.
	 *
	 * @return a <code>Set<String></code> value
	 */
	public final Set<String> getFieldNames() {
		return fieldNames;
	}

	/**
	 * Set the <code>FieldNames</code> value.
	 *
	 * @param newFieldNames The new FieldNames value.
	 */
	public final void setFieldNames(final Set<String> newFieldNames) {
		this.fieldNames = newFieldNames;
	}

	/**
	 * Get the <code>DocSink</code> value.
	 *
	 * @return a <code>DocSink</code> value
	 */
	public final DocSink getDocSink() {
		return docSink;
	}

	/**
	 * Set the <code>DocSink</code> value.
	 *
	 * @param newDocSink The new DocSink value.
	 */
	public final void setDocSink(final DocSink newDocSink) {
		this.docSink = newDocSink;
	}

	/**
	 * Get the <code>SourceCompression</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isSourceCompression() {
		return sourceCompression;
	}

	/**
	 * Set the <code>SourceCompression</code> value.
	 *
	 * @param newSourceCompression The new SourceCompression value.
	 */
	public final void setSourceCompression(final boolean newSourceCompression) {
		this.sourceCompression = newSourceCompression;
	}

	/**
	 * Get the <code>SinkCompression</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isSinkCompression() {
		return sinkCompression;
	}

	/**
	 * Set the <code>SinkCompression</code> value.
	 *
	 * @param newSinkCompression The new SinkCompression value.
	 */
	public final void setSinkCompression(final boolean newSinkCompression) {
		this.sinkCompression = newSinkCompression;
	}

	/**
	 * Get the <code>SourceProperties</code> value.
	 *
	 * @return a <code>Properties</code> value
	 */
	public final MultiValueProp getSourceProperties() {
		return sourceProperties;
	}

	/**
	 * Set the <code>SourceProperties</code> value.
	 *
	 * @param newSourceProperties The new SourceProperties value.
	 */
	public final void setSourceProperties(final MultiValueProp newSourceProperties) {
		this.sourceProperties = newSourceProperties;
	}

	/**
	 * Get the <code>SinkProperties</code> value.
	 *
	 * @return a <code>Properties</code> value
	 */
	public final MultiValueProp getSinkProperties() {
		return sinkProperties;
	}

	/**
	 * Set the <code>SinkProperties</code> value.
	 *
	 * @param newSinkProperties The new SinkProperties value.
	 */
	public final void setSinkProperties(final MultiValueProp newSinkProperties) {
		this.sinkProperties = newSinkProperties;
	}

	/**
	 * Get the <code>DocSource</code> value.
	 *
	 * @return a <code>DocSource</code> value
	 */
	public final DocSource getDocSource() {
		return docSource;
	}

	/**
	 * Set the <code>DocSource</code> value.
	 *
	 * @param newDocSource The new DocSource value.
	 */
	public final void setDocSource(final DocSource newDocSource) {
		this.docSource = newDocSource;
	}

	/**
	 * Get the <code>OutputChunk</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getOutputChunk() {
		return outputChunk;
	}

	/**
	 * Set the <code>OutputChunk</code> value.
	 *
	 * @param newOutputChunk The new OutputChunk value.
	 */
	public final void setOutputChunk(final int newOutputChunk) {
		this.outputChunk = newOutputChunk;
	}

	/**
	 * Get the <code>WorkPerThread</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getWorkPerThread() {
		return workPerThread;
	}

	/**
	 * Set the <code>WorkPerThread</code> value.
	 *
	 * @param newWorkPerThread The new WorkPerThread value.
	 */
	public final void setWorkPerThread(final int newWorkPerThread) {
		this.workPerThread = newWorkPerThread;
	}

	/**
	 * Get the <code>Userid</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getUserid() {
		return userid;
	}

	/**
	 * Set the <code>Userid</code> value.
	 *
	 * @param newUserid The new Userid value.
	 */
	public final void setUserid(final String newUserid) {
		this.userid = newUserid;
	}

	/**
	 * Get the <code>Password</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getPassword() {
		return password;
	}

	/**
	 * Set the <code>Password</code> value.
	 *
	 * @param newPassword The new Password value.
	 */
	public final void setPassword(final String newPassword) {
		this.password = newPassword;
	}


	Map<String, MultiValueProp> filterProperties = new HashMap<String, MultiValueProp>(2);
	public final void setFilterProperties(String n, MultiValueProp p)
	{
		filterProperties.put(n,p);
	}

	public Map<String, MultiValueProp> getFilterProperties()
	{
		return filterProperties;
	}

	static boolean cleanHalt = false;
	static public void cleanHalt()
	{
		cleanHalt = true;
	}

	/**
	 * Get the <code>SinkWriteToStdout</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isSinkWriteToStdout() {
		return sinkWriteToStdout;
	}

	/**
	 * Set the <code>SinkWriteToStdout</code> value.
	 *
	 * @param newSinkWriteToStdout The new SinkWriteToStdout value.
	 */
	public final void setSinkWriteToStdout(final boolean newSinkWriteToStdout) {
		this.sinkWriteToStdout = newSinkWriteToStdout;
	}

	/**
	 * Get the <code>DocPacketSize</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getDocPacketSize() {
		return docPacketSize;
	}

	public final void setDocPacketSize(final int newDocPacketSize) {
		docPacketSize = newDocPacketSize;
	}

	public final boolean isSinkReadFromStdin() {
		return sinkReadFromStdin;
	}

	public final void setSinkReadFromStdin(final boolean newSinkReadFromStdin) {
		this.sinkReadFromStdin = newSinkReadFromStdin;
	}

	public final float getLoadAverageLimit() {
		return loadAverageLimit;
	}

	public final void setLoadAverageLimit(final float newLoadAverageLimit) {
		this.loadAverageLimit = newLoadAverageLimit;
	}

	private void checkLoad()
	{
		LoadAvg.checkAvg(loadAverageLimit);
	}

}///////////////////////

