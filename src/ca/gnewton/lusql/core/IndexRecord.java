package ca.gnewton.lusql.core;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;

import java.util.*;

/**
   NOT USED
 * Describe class IndexRecord here.
 *
 *
 * Created: Fri Jan 11 15:55:11 2008
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public class IndexRecord {

    /**
     * Creates a new <code>IndexRecord</code> instance.
     *
     */
    public IndexRecord() {

    }

  public void index(IndexWriter writer, HashMap<String, List<String>>values, HashMap<String, LuceneFieldParameters> parameters)
	{
	  Document doc = new Document();
	  // loop through field:values
	  Iterator<String> it = values.keySet().iterator();
	  while(it.hasNext())
	    {
	      String field = it.next();
	    }
	}

}
