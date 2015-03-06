package ca.gnewton.lusql.core;
import org.apache.lucene.index.IndexWriter;
import java.util.*;
import java.io.*;
import org.apache.lucene.index.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.*;

/**
 * Describe class MergeIndexes here.
 *
 *
 * Created: Tue Sep 25 00:14:05 2007
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class MergeIndexes extends Thread {

	/**
	 * Describe items here.
	 */
	private int[] items;

	private String[] names;

	/**
	 * Describe mainName here.
	 */
	private String mainName;

	/**
	 * Describe analyzer here.
	 */
	private Analyzer analyzer;

	/**
	 * Describe result here.
	 */
	private IndexReader result;

	/**
	 * Describe resultName here.
	 */
	private String resultName;

	/**
	 * Describe indexes here.
	 */
	private IndexReader[] indexes;

	public void run()
	{
		try
			{
				System.out.println(this);
				if(indexes == null || indexes.length == 0)
					{
						setResult(null);
						return;
					}
				if(indexes.length <= 2)
					addItems(indexes, names);
				else
					recurse();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
				throw new NullPointerException();
			}
		System.out.println(Thread.currentThread().getName() + ": done");
	}

	void addItems(IndexReader[] lIndexes, String[] lNames)
		throws CorruptIndexException,  org.apache.lucene.store.LockObtainFailedException, IOException
	{
		if(lIndexes.length == 1)
			result = lIndexes[0];
		else
			{
		    
				IndexWriterConfig config = new IndexWriterConfig(LuSql.luceneVersion, 
				                                                 getAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		
				IndexWriter writer = new IndexWriter(lIndexes[0].directory(),
				                                     config);
				//IndexWriter writer = new IndexWriter(lIndexes[0].directory(), getAnalyzer(), true);
				IndexReader[] reader = new IndexReader[1];
				reader[0] = lIndexes[1];
				writer.addIndexes(reader);
				writer.close();
				lIndexes[1].close();
				lIndexes[0].close();
				result = IndexReader.open(FSDirectory.open(new File(lNames[0])));
			}
		resultName = lNames[0];
	}

	void recurse()
		throws CorruptIndexException,  org.apache.lucene.store.LockObtainFailedException, IOException	
	{
		MergeIndexes m1 = new MergeIndexes();
		m1.setAnalyzer(getAnalyzer());
		MergeIndexes m2 = new MergeIndexes();
		m2.setAnalyzer(getAnalyzer());
		m1.setIndexes(Arrays.copyOfRange(indexes, 0, indexes.length/2));
		m1.setNames(Arrays.copyOfRange(names, 0, names.length/2));
		m2.setIndexes(Arrays.copyOfRange(indexes, indexes.length/2, indexes.length));
		m2.setNames(Arrays.copyOfRange(names, names.length/2, names.length));
	    
		m1.start();
		m2.start();
		try
			{
				m1.join();
				m2.join();
			}
		catch(InterruptedException t)
			{
				t.printStackTrace();
			}
	    
		String[] lNames = new String[2];
		lNames[0] = m1.getResultName();
		lNames[1] = m2.getResultName();

		IndexReader[] lIndex= new IndexReader[2];
		lIndex[0] = m1.getResult();
		lIndex[1] = m2.getResult();
	    
		addItems(lIndex, lNames);
	}

	void setAddItems(int[] newItems)
	{
		items = newItems;
	}

	/**
	 * Get the <code>Result</code> value.
	 *
	 * @return an <code>IndexReader</code> value
	 */
	public final IndexReader getResult() {
		return result;
	}

	/**
	 * Set the <code>Result</code> value.
	 *
	 * @param newResult The new Result value.
	 */
	public final void setResult(final IndexReader newResult) {
		this.result = newResult;
	}
	/**
	 * Get the <code>Items</code> value.
	 *
	 * @return an <code>int[]</code> value
	 */
	public final int[] getItems() {
		return items;
	}

	/**
	 * Set the <code>Items</code> value.
	 *
	 * @param newItems The new Items value.
	 */
	public final void setItems(final int[] newItems) {
		this.items = newItems;
	}

	/**
	 * Describe <code>main</code> method here.
	 *
	 * @param args a <code>String</code> value
	 */
	public static final void main(final String[] args) 
	{
		MergeIndexes m = new MergeIndexes();
		try
			{
		
				m.setIndexes(m.makeReaders(args));
				m.setNames(args);
				m.run();
				//System.out.println("results=" + m.getResult());
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}
	}

	IndexReader[] makeReaders(String[] args)
		throws CorruptIndexException, IOException
	{
		IndexReader[] readers = new IndexReader[args.length];
		int count = 0;
		for(String dir: args)
			{
				readers[count++] = IndexReader.open(FSDirectory.open(new File(dir)));
			}
		return readers;
	}

	public String toString()
	{
		String s = new String();
		s += Thread.currentThread().getName();
		if(indexes == null)
			return s + " null";
		s += "\t";
		for(int i=0; i<names.length; i++)
			s += " " + names[i];
		return s;
	}


	/**
	 * Get the <code>Analyzer</code> value.
	 *
	 * @return an <code>Analyzer</code> value
	 */
	public final Analyzer getAnalyzer() {
		return analyzer;
	}

	/**
	 * Set the <code>Analyzer</code> value.
	 *
	 * @param newAnalyzer The new Analyzer value.
	 */
	public final void setAnalyzer(final Analyzer newAnalyzer) {
		this.analyzer = newAnalyzer;
	}

	/**
	 * Get the <code>Names</code> value.
	 *
	 * @return a <code>String[]</code> value
	 */
	public final String[] getNames() {
		return names;
	}

	/**
	 * Set the <code>Names</code> value.
	 *
	 * @param newNames The new Names value.
	 */
	public final void setNames(final String[] newNames) {
		this.names = newNames;
	}

	/**
	 * Get the <code>ResultName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getResultName() {
		return resultName;
	}

	/**
	 * Set the <code>ResultName</code> value.
	 *
	 * @param newResultName The new ResultName value.
	 */
	public final void setResultName(final String newResultName) {
		this.resultName = newResultName;
	}

	/**
	 * Get the <code>Indexes</code> value.
	 *
	 * @return an <code>IndexReader[]</code> value
	 */
	public final IndexReader[] getIndexes() {
		return indexes;
	}

	/**
	 * Set the <code>Indexes</code> value.
	 *
	 * @param newIndexes The new Indexes value.
	 */
	public final void setIndexes(final IndexReader[] newIndexes) {
		this.indexes = newIndexes;
	}
}
