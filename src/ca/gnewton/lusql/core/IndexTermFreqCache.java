package ca.gnewton.lusql.core;

import java.util.*;
import java.io.*;
import org.apache.lucene.index.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.store.*;

/**
 * Cache for Lucene Index Term Frequency
 *
 *
 * Created: Wed Aug 13 12:04:19 2008
 * Copyright 2008 National Research Council
 * License: GNU Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txtk
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a>CISTI Research, National Research Council
 * @version 0.9
 */
public class IndexTermFreqCache 
{

    /**
     * Describe reader here.
     */
    private IndexReader reader;

    /**
     * Describe fieldName here.
     */
    private String fieldName;

    /**
     * Describe preload here.
     */
    private boolean preload = false;

    Map<String, Integer>cache = null;
    public IndexTermFreqCache(final IndexReader newReader, 
			      final String newFieldName,
			      int initSize,
			      boolean newPreload)
	throws IOException
	{
	    setPreload(newPreload);
	    setReader(newReader);
	    setFieldName(newFieldName);
	    cache = new HashMap(initSize);
	    if(preload)
	    {
		TermEnum te = reader.terms();
		while(te.next())
		{
		    Term term = te.term();
		    /*
		    System.out.println(te.term().field()
				       + ": "
				       + te.term().text()
				       + ": "
				       + reader.docFreq(term));
		    */
		    if(term.field().equals(fieldName))
		    {
			cache.put(te.term().text(), new Integer(reader.docFreq(term)));


		    }
		}
	    }
	}

    public int count()
	{
	    return cache.size();
	}

    public int docFreq(final String termString)
	throws IOException	
	{
	    if(preload || cache.containsKey(termString))
		return cache.get(termString);
	    else
	    {
		Term tmpTerm = new Term(fieldName, termString);
		int freq = reader.docFreq(tmpTerm);
		cache.put(termString, new Integer(freq));
		return freq;
	    }
	}

    /**
     * Get the <code>Reader</code> value.
     *
     * @return an <code>IndexReader</code> value
     */
    public final IndexReader getReader() {
	return reader;
    }

    /**
     * Set the <code>Reader</code> value.
     *
     * @param newReader The new Reader value.
     */
    public final void setReader(final IndexReader newReader) {
	this.reader = newReader;
    }

    /**
     * Get the <code>FieldName</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getFieldName() {
	return fieldName;
    }

    /**
     * Set the <code>FieldName</code> value.
     *
     * @param newFieldName The new FieldName value.
     */
    public final void setFieldName(final String newFieldName) {
	this.fieldName = newFieldName;
    }

    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) {
	String dir = "itfcTestIndex";
	String cachedField = "title";
	try
	{
		IndexWriterConfig config = new IndexWriterConfig(LuSql.luceneVersion, 
		                                                 new StandardAnalyzer(LuSql.luceneVersion)).setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		
		IndexWriter writer = new IndexWriter(FSDirectory.open(new File(dir)),
		                                     config);
						 
	    // Doc #1
	    Document doc1 = new Document();
	    Field title1 = 
		new org.apache.lucene.document.Field(cachedField, "The Rain in Spain is plain",
						     Field.Store.NO, 
						     Field.Index.ANALYZED, 
						     Field.TermVector.YES);
	    doc1.add(title1);
	    org.apache.lucene.document.Field ab1 = 
		new org.apache.lucene.document.Field("ab", "This is the test abstract",
						     Field.Store.NO, 
						     Field.Index.ANALYZED, 
						     Field.TermVector.YES);
	    doc1.add(ab1);						 
	    writer.addDocument(doc1);


	    // Doc #2
	    Document doc2 = new Document();
	    Field title2 = 
		new org.apache.lucene.document.Field(cachedField, "This is the test plain title",
						     Field.Store.NO, 
						     Field.Index.ANALYZED, 
						     Field.TermVector.YES);
	    doc2.add(title2);
	    org.apache.lucene.document.Field ab2 = 
		new org.apache.lucene.document.Field("ab", "This is the test abstract",
						     Field.Store.NO, 
						     Field.Index.ANALYZED, 
						     Field.TermVector.YES);
	    doc2.add(ab2);						 
	    writer.addDocument(doc2);



	    writer.close();
	    
	    IndexReader reader = IndexReader.open(FSDirectory.open(new File(dir)));
	    IndexTermFreqCache cache = new IndexTermFreqCache(reader, cachedField, 100, true);
	    System.err.println(cache);
	}
	catch(Throwable t)
	{
	    t.printStackTrace();
	}

    }

    public String toString()
	{
	    StringBuffer sb = new StringBuffer();
	    Iterator<String> it = cache.keySet().iterator();
	    while(it.hasNext())
	    {
		String key = it.next();
		sb.append("\n" + getFieldName() + ":\t" + cache.get(key) + ":\t" + key);
	    }
	    return sb.toString();
	}

    /**
     * Get the <code>Preload</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isPreload() {
	return preload;
    }

    /**
     * Set the <code>Preload</code> value.
     *
     * @param newPreload The new Preload value.
     */
    public final void setPreload(final boolean newPreload) {
	this.preload = newPreload;
    }
} ///////

