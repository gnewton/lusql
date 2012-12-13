package ca.gnewton.lusql.driver.lucene;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.lucene.index.*;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.store.*;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.driver.concurrent.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class ConcurrentLuceneDocSink here.
 *
 *
 * Created: Mon Dec  1 17:38:28 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class ConcurrentLuceneDocSink
    extends LuceneDocSink
    implements LuSqlFields
{
    double nums[] = {3.0, 11.0, 17.0, 23.0, 29.0, 37.0};
    List<DocSink> docSinks = new ArrayList<DocSink>();
    List<String> indexNames = new ArrayList<String>();

    ThreadPoolExecutor tpe = null;

    public ConcurrentLuceneDocSink()
    {
	setThreaded(true);
    }

    @Override
    public void init(MultiValueProp p) 
	throws PluginException
	{
	    try
	    {
		extractProperties(p);
		openDocSinks(p); 
		BlockingQueue<Runnable> docQueue = new ArrayBlockingQueue<Runnable>(docSinks.size() * 2);
		//BlockingQueue<Runnable> docQueue = new ArrayBlockingQueue<Runnable>(32);
		//BlockingQueue<Runnable> docQueue = new ArrayBlockingQueue<Runnable>(500);
		
		tpe = new ThreadPoolExecutor(2, 
					     docSinks.size() * 5,
					     16l,
					     TimeUnit.SECONDS,
					     docQueue,
					     new ThreadPoolExecutor.CallerRunsPolicy());
	    }
	    catch(Throwable t)
	    {
		    //printDefaults();
		t.printStackTrace();
		throw new PluginException();
	    }
	}

    void openDocSinks(final MultiValueProp pp)
	throws PluginException
	{
	    System.err.println("pp=" + pp);
	    
	    for(int i=0; i<indexNames.size(); i++)
	    {
		MultiValueProp p = pp.copy();

		System.out.println("ConcurrentLuceneDocSink: Index: " + indexNames.get(i));
		System.err.println("p=" + p);
		// FIX
		DocSink newSink = new LuceneDocSink();

		p.setProperty(LuSqlFields.SinkLocationKey, indexNames.get(i));
		if(getRAMBufferSize() < 24)
		    p.setProperty(LuSqlFields.BufferSizeKey, Double.toString(getRAMBufferSize()));
		else
		    p.setProperty(LuSqlFields.BufferSizeKey, Double.toString(getRAMBufferSize() + ((i-2)*7)));
		if(i != 0)
		    {
			p.setProperty(LuSqlFields.CreateSinkKey, "true");
		    }
		newSink.init(p);
		if(i!= 0)
		    newSink.setRemoveOnDone(true);
		docSinks.add(newSink);
	    }
	}

    @Override
    public Properties explainProperties()
	{
	    Properties p = super.explainProperties();
	    String className = this.getClass().getName() + ".";
	    p.setProperty("secondaryIndexN", "Lucene index directories for parallel indexing. N=0....");
	    return p;
	}


    void extractProperties(MultiValueProp p)
	{
	    super.extractProperties(p);
	    indexNames.add(getLuceneIndexName());
	    for(int i=0; i<256; i++)
	    {
		if(p.containsKey(SecondarySinkLocationKey + i))
		    {
			indexNames.add(p.getProperty(SecondarySinkLocationKey + i).get(0));
			System.err.println("ConcurrentLuceneDocSink: extractProperties: "
					   + SecondarySinkLocationKey + i
					   + ":"
					   + p.getProperty(SecondarySinkLocationKey + i).get(0)
					   );
			
		    }
		
		else
		    break;
	    }


	}
    Random random;
    int t = 0;
    private final ReentrantLock lock = new ReentrantLock();
    public void addDoc(Doc[] doc)  
	throws DocSinkException
	{
	    DocAdd da = new DocAdd();
	    da.setDocs(doc);
	    lock.lock();
	    try
	    {
		if(t>=docSinks.size())
		    t = 0;
		da.setDocSink(docSinks.get(t));
		++t;
	    }
	    finally
	    {
		lock.unlock();
	    }
	    tpe.execute(da);
	}

    public Object internal()
	{
	    throw new NullPointerException();
	}

    public void commit2() throws DocSinkException
	{
	    try
	    {
		long tAll = System.currentTimeMillis();
		for(int i=1; i<docSinks.size(); i++)
		    {
			long t0 = System.currentTimeMillis();
			docSinks.get(i).commit();
			System.out.println("Time to commit: " 
					   + (System.currentTimeMillis()-t0)/1000);
		    }
		System.out.println("Time to commit all: " 
				   + (System.currentTimeMillis()-tAll)/1000);
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new DocSinkException();
	    }
	}


    public void commit() throws DocSinkException
	{
	    try
	    {
		ConcurrentDocSinkUtil[] dones = new ConcurrentDocSinkUtil[docSinks.size()];
		long tAll = System.currentTimeMillis();
		for(int i=1; i<docSinks.size(); i++)
		    {
			dones[i] = new ConcurrentDocSinkUtil(docSinks.get(i), ConcurrentDocSinkUtil.CAction.Commit);
			dones[i].start();
		    }
		for(int i=1; i<docSinks.size(); i++)
		    dones[i].join();

		System.out.println("Time to commit all: " 
				   + (System.currentTimeMillis()-tAll)/1000);
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new DocSinkException();
	    }
	}

    public void done()  
	throws PluginException
	{
	    try
	    {
		System.out.println("Shutting down pool");
		if(tpe != null)
		{
		    tpe.shutdown();
		    tpe.awaitTermination(500l,
					 TimeUnit.SECONDS);
		}
		commit();

		Directory[] merges = new Directory[docSinks.size()-1];
		
		IndexWriter finalIndex = (IndexWriter)(docSinks.get(0).internal());
		for(int i=1; i<docSinks.size(); i++)
		    {
			IndexWriter writer = ((IndexWriter)(docSinks.get(i).internal()));
			merges[i-1] = writer.getDirectory();
			docSinks.get(i).setRemoveOnDone(true);
		    }


		for(int i=1; i<docSinks.size(); i++)
		    docSinks.get(i).done();
		docSinks.get(0).done();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	}
	



    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) 
	{
	    MultiValueProp p = new MultiValueProp();
	    p.put("bufferSize", "16");

	    p.put("create", "true");
	    p.put("optimizeOnClose", "true");
	    p.put("index", "foo0");
	    p.put("secondaryIndex0", "foo1");
	    p.put("secondaryIndex1", "foo2");
	    p.put("analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");

	    ConcurrentLuceneDocSink ci = new ConcurrentLuceneDocSink();
	    try
	    {
		ci.init(p);
		
		LuceneFieldParameters lfp = new LuceneFieldParameters();
		for(int i=0; i<1000000; i++)
		{
		    Doc d = new DocImp();
		    d.addField("name", "n" + i, lfp);
		    //ci.addDoc(d);
		}
		ci.done();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	}


} ///////
