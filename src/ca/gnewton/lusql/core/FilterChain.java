package ca.gnewton.lusql.core;

import org.apache.lucene.document.*;
import java.util.*;


/**
 * Describe class FilterChain here.
 *
 *
 * Created: Sat Jan 19 11:29:17 2008
 *
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public class FilterChain 
 extends NullFilter
{
  public Doc filter(Doc doc)
	{
	    Iterator<DocFilter> it = filterList.iterator();
	    while(it.hasNext())
	    {
		try
		{	    
		    doc = it.next().filter(doc);
		}
		catch(Throwable t)
		{
		    //getLuSql().setFatalError(true);
		    return null;
		}
	    }

	    return doc;
	}
    Properties props = null;
    
    List<DocFilter> filterList = new ArrayList<DocFilter>();

    void addFilter(DocFilter df)
	{
	    filterList.add(df);
	}

}
