package ca.gnewton.lusql.example;
import org.apache.lucene.document.*;
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.util.zip.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Describe class CompassFilter here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */

public class CompassFilter 
    extends DBDocFilterImp
{
    public Doc filter(Doc doc)
	{
	    if(doc == null)
		return null;


	    doc = changeToCompassFieldNames(doc);

	    return doc;
	}


    // Changes fields names from "foo" to "<foo>"
    static LuceneFieldParameters paras = new LuceneFieldParameters(Field.Index.NO, Field.Store.YES, Field.TermVector.NO);
    Doc changeToCompassFieldNames(Doc doc)
	{
	    String value = (doc.getFieldValues("alias")).get(0);


	    String idField = "$/" + value + "/id";
	    String uidField = "$/uid";

	    int n = getCounter();

	    doc.addField(idField, ""+n, paras);
	    doc.addField(uidField, value + "#" + n + "#", paras);
	    return doc;
	}

    static int counter = 0;
    static private final ReentrantLock lock = new ReentrantLock();
    static int getCounter()
	{
	    lock.lock();  // block until condition holds
	    try {
		counter++;
	    } finally {
		lock.unlock();
	    }
	    return counter;
	}

}//////////
