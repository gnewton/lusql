package ca.gnewton.lusql.core;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Describe class AddDocument here.
 *
 *
 * Created: Mon Jul 21 14:38:12 2008
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 */
public class AddDocument implements Runnable 
{
    static private final ReentrantLock lock = new ReentrantLock();    
    boolean threadSubQueries = false;
    /**
     * Describe filter here.
     */
    static private DocFilter filter;

    /**
     * Describe subQueryJoinFilters here.
     */
    static private SubQueryJoinFilter[] subQueryJoinFilters;

    Doc docs[] = null;

    /**
     * Describe luSql here.
     */
    private LuSql luSql;


    /**
     * Describe docSink here.
     */
    static private DocSink docSink;

    /**
     * Describe filters here.
     */
    static private List<DocFilter> filters;

    /**
     * Creates a new <code>AddDocument</code> instance.
     *
     */
    static int count = 0;
    public AddDocument()
	{

	}



// Implementation of java.lang.Runnable

    /**
     * Describe <code>run</code> method here.
     *
     */

    public final void run() 
	{
	    if(docSink == null)
		System.err.println("AddDocument: DocSink is null!!!!!!  9898");

	    if(docs == null || getLuSql().isFatalError())
		return;

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
				//System.err.println("Running filter: " + filter.getClass().getName());
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
		    
		   
		    count++;
		    if(count%10000==0)
			{
			    Runtime.getRuntime().runFinalization();
			    Runtime.getRuntime().gc();
			}
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
		    //luSql.returnDoc(doc);
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
