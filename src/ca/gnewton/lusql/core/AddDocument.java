package ca.gnewton.lusql.core;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicLong;
import ca.gnewton.lusql.util.LoadAvg;

/**
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a>
 * @copyright CISTI / National Research Council Canada & Glen Newton
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 */
public class AddDocument implements Runnable 
{
	static private final ReentrantLock lock = new ReentrantLock();    
	private boolean threadSubQueries = false;
	static private DocFilter filter;

	static private SubQueryJoinFilter[] subQueryJoinFilters;
	private Doc docs[] = null;
	private LuSql luSql;
	static private DocSink docSink;
	static private List<DocFilter> filters;

	
	private static volatile AtomicLong count = new AtomicLong(0);
	//static int count = 0;
	public AddDocument()
	{

	}


	@Override
	public final void run() 
	{
		
		if(docSink == null)
			System.err.println("AddDocument: DocSink is null!!!!!!  9898");

		if(docs == null || getLuSql().isFatalError())
			return;

		LoadAvg.checkAvg(2.5);
		Doc doc = null;
		
		for(int i=0; i<docs.length; i++)
			{
				doc = docs[i];
				if(doc == null || docs[i].isLast())
					{
						continue;
					}
				//System.err.println("AddDocument " + i + " " + doc.getFieldValues("i").get(0));
				try
					{	
						doc = runSubQueryFilters(doc);
					}
				catch(Throwable t)
					{
						getLuSql().setFatalError(true);
						return;
					}
				if(filters != null && filters.size() > 0)
					{
						try
							{
								for(DocFilter filter: filters)
									{
										doc = filter.filter(doc);
									}
							}
						catch(Throwable t)
							{
								t.printStackTrace();
								getLuSql().setFatalError(true);
								return;
							}
					}
				docs[i] = doc;
			}
	    
	    
		try
			{
				if(docSink == null)
					System.err.println("AddDocument: DocSink is null!!!!!!");
				//List<Doc>dd = new ArrayList<Doc>(docs.length);
				//for(int i=0; i<docs.length; i++)
				//if(docs[i] != null && !docs[i].isLast())
				//dd.add(docs[i]);
				//docSink.addDoc(dd);		

				boolean notThreadSafeSink = ! docSink.isThreadSafe();
				if(notThreadSafeSink)
					lock.lock();
				try
					{
						docSink.addDoc(docs);		
					}
				finally
					{
						if(notThreadSafeSink)
							lock.unlock();
					}
		    
		   
				count.incrementAndGet();
			}
		catch(Throwable t)
			{
				System.err.println(doc);
				t.printStackTrace();
				getLuSql().setFatalError(true);
				throw new NullPointerException();
			}
		finally
			{
				getLuSql().incrementRealCount();
			}
	}
    


	/**
	 * Get the <code>Filter</code> value.
	 *
	 * @return a <code>DocumentFilter</code> value
	 */
	static public final DocFilter getFilter() {
		return filter;
	}

	/**
	 * Set the <code>Filter</code> value.
	 *
	 * @param newFilter The new Filter value.
	 */
	static public final void setFilter(final DocFilter newFilter) {
		filter = newFilter;
	}


	Doc runSubQueryFilters(Doc doc)
		throws ca.gnewton.lusql.core.FatalFilterException
	{
		//if(subQueryJoinFilters == null)
		//System.err.println("SubQuery null");

		if(subQueryJoinFilters != null)
			{
				//System.err.println("SubFilter running");
				//Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
		
				if(!threadSubQueries)
					{
						for(int i=0; i<subQueryJoinFilters.length; i++)
							{
								doc = subQueryJoinFilters[i].filter(doc);
							}
					}
				else
					{
						Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
						//Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
						try
							{
								Thread[] threads = new Thread[subQueryJoinFilters.length];
								for(int i=0; i<subQueryJoinFilters.length; i++)
									{
										System.err.println("SubQueryFilters:" + i);
										//document = 
										SubQueryRunnable sqr = new SubQueryRunnable();
										sqr.setDoc(doc);
										sqr.setSubQueryJoinFilter(subQueryJoinFilters[i]);
										Thread t = new Thread(sqr);
										threads[i] = t;
										t.start();
										if(false)
											{
												try
													{
														t.join();
													}
												catch(InterruptedException ex)
													{
														ex.printStackTrace();
													}
											}
									}
								for(int i=0; i<subQueryJoinFilters.length; i++)
									{
										try
											{
												threads[i].join();
											}
										catch(InterruptedException ex)
											{
												ex.printStackTrace();
											}
									}
							}
						finally
							{
								Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
							}
					}
			}
		return doc;
	}


	static int numDestroyed = 0;
	protected void finalize() throws Throwable 
	{
		try 
			{
				numDestroyed++;
			} 
		finally {
			super.finalize();
		}
	}

	/**
	 * Get the <code>SubQueryJoinFilters</code> value.
	 *
	 * @return a <code>SubQueryJoinFilter[]</code> value
	 */
	static public final SubQueryJoinFilter[] getSubQueryJoinFilters() {
		return subQueryJoinFilters;
	}

	/**
	 * Set the <code>SubQueryJoinFilters</code> value.
	 *
	 * @param newSubQueryJoinFilters The new SubQueryJoinFilters value.
	 */
	static public final void setSubQueryJoinFilters(final SubQueryJoinFilter[] newSubQueryJoinFilters) {
		subQueryJoinFilters = newSubQueryJoinFilters;
	}

	/**
	 * Set the <code>LuSql</code> value.
	 *
	 * @param newLuSql The new LuSql value.
	 */
	public final void setLuSql(final LuSql newLuSql) {
		this.luSql = newLuSql;
	}

	/**
	 * Get the <code>LuSql</code> value.
	 *
	 * @return a <code>LuSql</code> value
	 */
	public final LuSql getLuSql() {
		return luSql;
	}

	public void setDocs(Doc[] newDocs)
	{
		docs = newDocs;
	}


	/**
	 * Get the <code>DocSink</code> value.
	 *
	 * @return a <code>DocSink</code> value
	 */
	static public final DocSink getDocSink() {
		return docSink;
	}

	/**
	 * Set the <code>DocSink</code> value.
	 *
	 * @param newDocSink The new DocSink value.
	 */
	static public final void setDocSink(final DocSink newDocSink) {
		docSink = newDocSink;
	}

	/**
	 * Get the <code>Filters</code> value.
	 *
	 * @return a <code>List<DocFilter></code> value
	 */
	static public final List<DocFilter> getFilters() {
		return filters;
	}

	/**
	 * Set the <code>Filters</code> value.
	 *
	 * @param newFilters The new Filters value.
	 */
	static public final void setFilters(final List<DocFilter> newFilters) {
		filters = newFilters;
	}
} /////////
