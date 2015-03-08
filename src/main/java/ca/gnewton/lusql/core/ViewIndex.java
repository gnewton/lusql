package ca.gnewton.lusql.core;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import java.util.*;
import java.io.*;
import org.apache.lucene.store.FSDirectory;

/**
 * Describe class ViewIndex here.
 *
 *
 * Created: Sat Jan 19 10:17:33 2008
 *
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 */
public class ViewIndex 
{

    /**
     * Creates a new <code>ViewIndex</code> instance.
     *
     */
    public ViewIndex() 
	{
	    
	}
    
    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) 
	{
	    try
	    {
		IndexReader reader = IndexReader.open(FSDirectory.open(new File(args[0])));	    
		System.out.println("# of documents indexed: " + (reader.maxDoc()-1));

		int maxDoc = reader.maxDoc();
		for(int i=0; i<maxDoc; i++)
		{
		    Document doc = reader.document(i);
		    printDoc(doc, i);

		}

		reader.close();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }

	    
	}

    static void printDoc(Document doc, int i)
	{
	    System.out.println("\nDocument#: " + i);
	    List list = doc.getFields();
	    Iterator fields = list.iterator();
	    while(fields.hasNext())
	    {
		Field field = (Field)fields.next();
		System.out.println("\t" + field.name() + ": [" + field.stringValue() + "]");
	    }
	}
}
