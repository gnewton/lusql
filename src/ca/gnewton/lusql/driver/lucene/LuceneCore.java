package ca.gnewton.lusql.driver.lucene;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;

import java.io.*;

import java.lang.reflect.Constructor;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;


/**
 * Describe class LuceneCore here.
 *
 *
 * Created: Tue Dec 23 15:45:17 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
abstract public class LuceneCore 
{
	public static final String DEFAULT_ANALYZER="org.apache.lucene.analysis.standard.StandardAnalyzer";
	
	boolean debug = false;
	/**
	 * Describe query here.
	 */
	private String query=null;

	/**
	 * Describe SourceLocation here.
	 */
	private String SourceLocation = null;

	/**
	 * Describe appendToSink here.
	 */
	private boolean appendToSink = false;
	/**
	 * Describe RAMBufferSize here.
	 */
	protected double RAMBufferSize=32.0;

	/**
	 * Describe indexCreate here.
	 */
	protected boolean indexCreate=true;

	/**
	 * Describe analyzerName here.
	 */
	protected String analyzerName;

	/**
	 * Describe luceneIndexName here.
	 */
	protected String luceneIndexName;

	/**
	 * Describe stopWordFileName here.
	 */
	protected String stopWordFileName;

	/**
	 * Describe optimizeOnClose here.
	 */
	protected boolean optimizeOnClose=true;
 
	/**
	 * Describe removeOnDone here.
	 */
	protected boolean removeOnDone=false;


	public LuceneCore() {

	}


	/**
	 * Get the <code>RAMBufferSize</code> value.
	 *
	 * @return a <code>double</code> value
	 */
	public final double getRAMBufferSize() {
		return RAMBufferSize;
	}

	/**
	 * Set the <code>RAMBufferSize</code> value.
	 *
	 * @param newRAMBufferSize The new RAMBufferSize value.
	 */
	public final void setRAMBufferSize(final double newRAMBufferSize) {
		this.RAMBufferSize = newRAMBufferSize;
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
	 * Get the <code>AnalyzerName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getAnalyzerName() {
		return analyzerName;
	}

	/**
	 * Set the <code>AnalyzerName</code> value.
	 *
	 * @param newAnalyzerName The new AnalyzerName value.
	 */
	public final void setAnalyzerName(final String newAnalyzerName) {
		this.analyzerName = newAnalyzerName;
	}

	/**
	 * Get the <code>LuceneIndexName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getLuceneIndexName() {
		return luceneIndexName;
	}

	/**
	 * Set the <code>LuceneIndexName</code> value.
	 *
	 * @param newLuceneIndexName The new LuceneIndexName value.
	 */
	public final void setLuceneIndexName(final String newLuceneIndexName) {
		this.luceneIndexName = newLuceneIndexName;
	}

 
	void luceneExtractProperties(MultiValueProp p)
	{
		if(p.containsKey(LuSqlFields.BufferSizeKey))
			setRAMBufferSize(Double.parseDouble(p.getProperty(LuSqlFields.BufferSizeKey).get(0)));
		db("RAMBufferSize", Double.toString(getRAMBufferSize()));

		if(p.containsKey(LuSqlFields.CreateSinkKey))
			setIndexCreate(Boolean.parseBoolean(p.getProperty(LuSqlFields.CreateSinkKey).get(0)));
		db("Create Index:", Boolean.toString(isIndexCreate()));

		if(p.containsKey(LuSqlFields.SinkLocationKey))
			setLuceneIndexName(p.getProperty(LuSqlFields.SinkLocationKey).get(0));
	    
		db("SinkLocation", getLuceneIndexName());

		if(p.containsKey(LuSqlFields.AnalyzerClassKey))
			setAnalyzerName(p.getProperty(LuSqlFields.AnalyzerClassKey).get(0));
	    
		db("AnalyzerClass", getAnalyzerName()); 

		if(p.containsKey(LuSqlFields.StopWordFileNameKey))
			setStopWordFileName(p.getProperty(LuSqlFields.StopWordFileNameKey).get(0));
		db("StopWordFile", getStopWordFileName());

		if(p.containsKey(LuSqlFields.QueryKey))
			setQuery(p.getProperty(LuSqlFields.QueryKey).get(0));
		db("Query", getQuery());

		if(p.containsKey(LuSqlFields.SourceLocationKey))
			setSourceLocation(p.getProperty(LuSqlFields.SourceLocationKey).get(0));
		db("SourceLocation", getSourceLocation());


		if(p.containsKey(LuSqlFields.AppendToSinkKey))
			setAppendToSink(Boolean.parseBoolean(p.getProperty(LuSqlFields.AppendToSinkKey).get(0)));
		db("RemoveSinksOnDone", "" + isRemoveOnDone());

	}


	public void db(String w, String v)
	{
		/*
		  if(debug)
		  System.err.println(this.getClass().getName() 
		  + ":"
		  + w
		  + ":"
		  + v);
		*/
	}


	Analyzer newAnalyzer()
		throws ClassNotFoundException,
		       NoSuchMethodException,
		       InstantiationException,
		       IllegalAccessException,
		       IOException,
		       java.lang.reflect.InvocationTargetException
	{
		//System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
		//System.out.println("*******************************LuceneCore. newAnalyzer(): " + getAnalyzerName());
		/// FIXXXXXXXXXXXX
		//if(true)
		//return new SnowballAnalyzer("English", loadStopwordFile(getStopWordFileName()));
		Class<?> analyserClass = Class.forName(getAnalyzerName());

		Class<?> fileClass = null;
		if(getStopWordFileName() != null)
			{
				if(getAnalyzerName().equals(DEFAULT_ANALYZER))
					{
						Class[] paras = new Class[2];
						paras[0] = LuSql.luceneVersion.getClass();
						Reader swReader = stopWordReader(getStopWordFileName());
						paras[1] = swReader.getClass();

						Constructor constructor = analyserClass.getConstructor(paras);
						return (Analyzer)constructor.newInstance(LuSql.luceneVersion,
						                                         swReader);
					}
				else
					{
						Constructor constructor = analyserClass.getConstructor(fileClass);
						return (Analyzer)constructor.newInstance(new File(getStopWordFileName()));
					}
			}
		else
			{
				Constructor constructor = null;

				//if(getAnalyzerName().equals("org.apache.lucene.analysis.snowball.SnowballAnalyzer"))
				if(getAnalyzerName().equals(DEFAULT_ANALYZER))
					{
						//System.err.println("No stop/yes snow");
						Class[] paras = new Class[1];

						// We are matching the constructors parameters
						// SnowballAnalyzer(org.apache.lucene.util.Version matchVersion, String name) 
						paras[0] = LuSql.luceneVersion.getClass();
						constructor = analyserClass.getConstructor(paras);
						//return (Analyzer)constructor.newInstance(Version.LUCENE_40,
						return (Analyzer)constructor.newInstance(LuSql.luceneVersion);
					}
				else
					{
						//System.out.println("no stop/no snow");
						constructor = analyserClass.getConstructor();
						return (Analyzer)constructor.newInstance();
					}
			}
	}

	Reader stopWordReader(String swFileName)
		throws FileNotFoundException
	{
		return new BufferedReader(new FileReader(swFileName));		
	}
	
	public final String getStopWordFileName() {
		return stopWordFileName;
	}

	public final void setStopWordFileName(final String newStopWordFileName) {
		this.stopWordFileName = newStopWordFileName;
	}

	public boolean isThreaded()
	{
		return false;
	}

	public final boolean isRemoveOnDone() {
		return removeOnDone;
	}

	public final String getQuery() {
		return query;
	}

	public final void setQuery(final String newQuery) {
		this.query = newQuery;
	}

	public final String getSourceLocation() {
		return SourceLocation;
	}

	public final void setSourceLocation(final String newSourceLocation) {
		this.SourceLocation = newSourceLocation;
	}

	public final boolean isAppendToSink() {
		return appendToSink;
	}

	/**
	 * Set the <code>AppendToSink</code> value.
	 *
	 * @param newAppendToSink The new AppendToSink value.
	 */
	public final void setAppendToSink(final boolean newAppendToSink) {
		this.appendToSink = newAppendToSink;
	}

	public final boolean getAppendToSink() 
	{
		return appendToSink;
	}
}
