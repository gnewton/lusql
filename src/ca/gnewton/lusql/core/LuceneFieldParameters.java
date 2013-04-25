package ca.gnewton.lusql.core;

import org.apache.lucene.document.*;
import java.util.*;

/**
 * Describe class LuceneFieldParameters here.
 *
 *
 * Created: Fri Jan 11 15:11:30 2008
 *
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public class LuceneFieldParameters 
{
    static public Map<String, Field.Index>indexx = new HashMap<String, Field.Index>();
    static public Map<Field.Index, String>rindex = new HashMap<Field.Index,String>();
    static public Map<String, Field.Store>storex = new HashMap<String, Field.Store>();
    static public Map<Field.Store,String>rstorex = new HashMap<Field.Store,String>();
    static public Map<String, Field.TermVector>termx = new HashMap<String, Field.TermVector>();
    static public Map<Field.TermVector, String>rtermx = new HashMap<Field.TermVector, String>();
    
     {
	 indexx.put("ANALYZED",  Field.Index.ANALYZED);
	 rindex.put(Field.Index.ANALYZED, "ANALYZED");
	 indexx.put("Index.ANALYZED",  Field.Index.ANALYZED);
	 rindex.put(Field.Index.ANALYZED, "Index.ANALYZED");
	 indexx.put("ANALYZED_NO_NORMS", Field.Index.ANALYZED_NO_NORMS); 
	 rindex.put(Field.Index.ANALYZED_NO_NORMS, "ANALYZED_NO_NORMS");
	 indexx.put("Index.ANALYZED_NO_NORMS", Field.Index.ANALYZED_NO_NORMS); 
	 rindex.put(Field.Index.ANALYZED_NO_NORMS, "Index.ANALYZED_NO_NORMS");
	 indexx.put("NO", Field.Index.NO); 
	 rindex.put(Field.Index.NO, "NO"); 
	 indexx.put("Index.NO", Field.Index.NO); 
	 rindex.put(Field.Index.NO, "Index.NO"); 
	 indexx.put("NOT_ANALYZED", Field.Index.NOT_ANALYZED); 
	 rindex.put(Field.Index.NOT_ANALYZED, "NOT_ANALYZED"); 
	 indexx.put("Index.NOT_ANALYZED", Field.Index.NOT_ANALYZED); 
	 rindex.put(Field.Index.NOT_ANALYZED, "Index.NOT_ANALYZED"); 
	 indexx.put("NOT_ANALYZED_NO_NORMS", Field.Index.NOT_ANALYZED_NO_NORMS); 
	 rindex.put(Field.Index.NOT_ANALYZED_NO_NORMS, "NOT_ANALYZED_NO_NORMS"); 
	 indexx.put("Index.NOT_ANALYZED_NO_NORMS", Field.Index.NOT_ANALYZED_NO_NORMS); 
	 rindex.put(Field.Index.NOT_ANALYZED_NO_NORMS, "Index.NOT_ANALYZED_NO_NORMS"); 

	 storex.put("YES", Field.Store.YES);
	 rstorex.put(Field.Store.YES, "YES");
	 storex.put("Store.YES", Field.Store.YES);
	 rstorex.put(Field.Store.YES, "Store.YES");
	 storex.put("NO", Field.Store.NO);
	 rstorex.put(Field.Store.NO, "NO");
	 storex.put("Store.NO", Field.Store.NO);
	 rstorex.put(Field.Store.NO, "Store.NO");
	 
	 termx.put("NO", Field.TermVector.NO); 
	 rtermx.put(Field.TermVector.NO, "NO"); 
	 termx.put("TermVector.NO", Field.TermVector.NO); 
	 rtermx.put(Field.TermVector.NO, "TermVector.NO"); 
	 termx.put("WITH_OFFSETS", Field.TermVector.WITH_OFFSETS); 
	 rtermx.put(Field.TermVector.WITH_OFFSETS, "WITH_OFFSETS"); 
	 termx.put("TermVector.WITH_OFFSETS", Field.TermVector.WITH_OFFSETS); 
	 rtermx.put(Field.TermVector.WITH_OFFSETS, "TermVector.WITH_OFFSETS"); 
	 termx.put("WITH_POSITIONS_OFFSETS", Field.TermVector.WITH_POSITIONS_OFFSETS);
	 rtermx.put(Field.TermVector.WITH_POSITIONS_OFFSETS, "WITH_POSITIONS_OFFSETS");
	 termx.put("TermVector.WITH_POSITIONS_OFFSETS", Field.TermVector.WITH_POSITIONS_OFFSETS);
	 rtermx.put(Field.TermVector.WITH_POSITIONS_OFFSETS, "TermVector.WITH_POSITIONS_OFFSETS");
	 termx.put("WITH_POSITIONS_OFFSETS", Field.TermVector. WITH_POSITIONS_OFFSETS); 
	 rtermx.put(Field.TermVector. WITH_POSITIONS_OFFSETS, "WITH_POSITIONS_OFFSETS"); 
	 termx.put("TermVector.WITH_POSITIONS_OFFSETS", Field.TermVector. WITH_POSITIONS_OFFSETS); 
	 rtermx.put(Field.TermVector. WITH_POSITIONS_OFFSETS, "TermVector.WITH_POSITIONS_OFFSETS"); 
	 termx.put("YES", Field.TermVector.YES); 
	 rtermx.put(Field.TermVector.YES, "YES"); 
	 termx.put("TermVector.YES", Field.TermVector.YES); 
	 rtermx.put(Field.TermVector.YES, "TermVector.YES"); 
     }
    /**
     * Describe store here.
     */
    private org.apache.lucene.document.Field.Store store;

    /**
     * Describe index here.
     */
    private org.apache.lucene.document.Field.Index index;

    /**
     * Describe termVector here.
     */
    private org.apache.lucene.document.Field.TermVector termVector;

    /**
     * Creates a new <code>LuceneFieldParameters</code> instance.
     *
     */

    public LuceneFieldParameters(Field.Index newIndex,
				 Field.Store newStore,
				 Field.TermVector newTermVector) 
	{
	    setStore(newStore);
	    setIndex(newIndex);
	    setTermVector(newTermVector);
	}


  public LuceneFieldParameters()
	{
	    setIndex(Field.Index.ANALYZED);
	    setStore(Field.Store.YES);
	    setTermVector(Field.TermVector.NO);;
	}

    // assumes 3 chars
    public LuceneFieldParameters(String s)
    {
	String[] parts = s.split(":");
	if(parts.length != 3)
	    {
		System.err.println("Error. Problem with lucene field parameters: " 
				   + s);
		throw new NullPointerException();
	    }
	
		    

	if(!indexx.containsKey(parts[0])
	   || !storex.containsKey(parts[1])
	   || !termx.containsKey(parts[2])
	   )
	    {
		throw new NullPointerException();
	    }
	setIndex(indexx.get(parts[0]));
	setStore(storex.get(parts[1]));
	setTermVector(termx.get(parts[2]));

	//System.out.println("LuceneFieldParameters : " + getIndex());
	//System.out.println("LuceneFieldParameters : " + getStore());
	//System.out.println("LuceneFieldParameters : " + getTermVector());
	

    }

    /**
     * Get the <code>Store</code> value.
     *
     * @return an <code>org.apache.lucene.document.Field.Store</code> value
     */
    public final org.apache.lucene.document.Field.Store getStore() {
	return store;
    }

    /**
     * Set the <code>Store</code> value.
     *
     * @param newStore The new Store value.
     */
    public final void setStore(final org.apache.lucene.document.Field.Store newStore) {
	this.store = newStore;
    }

    /**
     * Get the <code>Index</code> value.
     *
     * @return an <code>org.apache.lucene.document.Field.Index</code> value
     */
    public final org.apache.lucene.document.Field.Index getIndex() {
	return index;
    }

    /**
     * Set the <code>Index</code> value.
     *
     * @param newIndex The new Index value.
     */
    public final void setIndex(final org.apache.lucene.document.Field.Index newIndex) {
	this.index = newIndex;
    }

    /**
     * Get the <code>TermVector</code> value.
     *
     * @return an <code>org.apache.lucene.document.Field.TermVector</code> value
     */
    public final org.apache.lucene.document.Field.TermVector getTermVector() {
	return termVector;
    }

    /**
     * Set the <code>TermVector</code> value.
     *
     * @param newTermVector The new TermVector value.
     */
    public final void setTermVector(final org.apache.lucene.document.Field.TermVector newTermVector) {
	this.termVector = newTermVector;
    }

    public String toString()
	{
	    return getIndex() + ":" + getStore() + ":" + getTermVector();
	}
}
