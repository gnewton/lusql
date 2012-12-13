package ca.gnewton.lusql.core;
import org.apache.lucene.index.IndexWriter;
import java.util.*;

/**
 * Describe class Merge here.
 *
 *
 * Created: Tue Sep 25 00:14:05 2007
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class Merge extends Thread {

    /**
     * Describe result here.
     */
    private int result;

    /**
     * Describe items here.
     */
    private int[] items;

    public void run()
	{
	    System.out.println(this);
	    if(items == null)
		{
		    setResult(0);
		    return;
		}
	    if(items.length <= 2)
		addItems();
	    else
		recurse();
	}

    void addItems()
	{
	    int sum=0;
	    for(int i=0; i<items.length; i++)
		sum += items[i];
	    result = sum;
	}

    void recurse()
	{
	    Merge m1 = new Merge();
	    Merge m2 = new Merge();
	    m1.setItems(Arrays.copyOfRange(items, 0, items.length/2));
	    m2.setItems(Arrays.copyOfRange(items, items.length/2, items.length));
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
	    result = m1.getResult() + m2.getResult();
			
			
	}

    void setAddItems(int[] newItems)
	{
	    items = newItems;
	}

    /**
     * Get the <code>Result</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getResult() {
	return result;
    }

    /**
     * Set the <code>Result</code> value.
     *
     * @param newResult The new Result value.
     */
    public final void setResult(final int newResult) {
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
	    int i[] = {1,2,3,4,5,6, 10};
	    Merge m = new Merge();
	    m.setItems(i);
	    m.run();
	    System.out.println("results=" + m.getResult());
	}

    public String toString()
	{
	    String s = new String();
	    s += Thread.currentThread().getName();
	    if(items == null)
		return s + " null";
	    s += "\t";
	    for(int i=0; i<items.length; i++)
		s += " " + items[i];
	    return s;
	}
}
