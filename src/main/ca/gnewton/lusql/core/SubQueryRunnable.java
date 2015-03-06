package ca.gnewton.lusql.core;
import org.apache.lucene.document.*;

/**
 * Describe class SubQueryRunnable here.
 *
 *
 * Created: Fri Oct 31 10:51:56 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class SubQueryRunnable implements Runnable {

    /**
     * Describe document here.
     */
    private Doc doc;

    /**
     * Describe subQueryJoinFilter here.
     */
    private SubQueryJoinFilter subQueryJoinFilter;

    /**
     * Creates a new <code>SubQueryRunnable</code> instance.
     *
     */
    public SubQueryRunnable() {

    }

// Implementation of java.lang.Runnable

    /**
     * Describe <code>run</code> method here.
     *
     */
    public final void run() {
	try
	{
	    subQueryJoinFilter.filter(doc);
	}
	catch(Throwable t)
	{
	    t.printStackTrace();
	}
    }

    /**
     * Get the <code>Doc</code> value.
     *
     * @return a <code>Doc</code> value
     */
    public final Doc getDoc() {
	return doc;
    }

    /**
     * Set the <code>Doc</code> value.
     *
     * @param newDoc The new Doc value.
     */
    public final void setDoc(final Doc newDoc) {
	this.doc = newDoc;
    }

    /**
     * Get the <code>SubQueryJoinFilter</code> value.
     *
     * @return a <code>SubQueryJoinFilter</code> value
     */
    public final SubQueryJoinFilter getSubQueryJoinFilter() {
	return subQueryJoinFilter;
    }

    /**
     * Set the <code>SubQueryJoinFilter</code> value.
     *
     * @param newSubQueryJoinFilter The new SubQueryJoinFilter value.
     */
    public final void setSubQueryJoinFilter(final SubQueryJoinFilter newSubQueryJoinFilter) {
	this.subQueryJoinFilter = newSubQueryJoinFilter;
    }
}
