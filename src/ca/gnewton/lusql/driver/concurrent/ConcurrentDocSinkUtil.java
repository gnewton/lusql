package ca.gnewton.lusql.driver.concurrent; 
import org.apache.lucene.index.IndexWriter;
import ca.gnewton.lusql.core.*;

/**
 * Describe class ConcurrentDocSinkUtil here.
 *
 *
 * Created: Tue Sep 25 00:14:05 2007
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class ConcurrentDocSinkUtil
    extends Thread 
{
    public enum CAction
	{
	    Done,
	    Commit
	}
    /**
     * Describe docSink here.
     */
    private DocSink docSink;

    /**
     * Describe action here.
     */
    private CAction action;

    /**
     * Creates a new <code>ConcurrentDocSinkUtil</code> instance.
     *
     */
    public ConcurrentDocSinkUtil(final DocSink newDocSink, CAction newAction) {
	setDocSink(newDocSink);
	setAction(newAction);
    }

    public void run()
	{
	    try
	    { 
		switch(action)
		{
		    case Done:
			getDocSink().done();
			break;
		    case Commit:
			getDocSink().commit();
			break;
		}
	    }
	    catch(Throwable t)
	    {
		System.err.println("Problem with DocSink: " 
				   + getDocSink().description()
		    );

		t.printStackTrace();
	    }

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
     * Get the <code>Action</code> value.
     *
     * @return an <code>Action</code> value
     */
    public final CAction getAction() {
	return action;
    }

    /**
     * Set the <code>Action</code> value.
     *
     * @param newAction The new Action value.
     */
    public final void setAction(final CAction newAction) {
	this.action = newAction;
    }
}
