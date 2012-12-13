package ca.gnewton.lusql.filters;
import org.apache.lucene.document.*;
import java.util.*;
import ca.gnewton.lusql.core.*;

/**
 * Describe class PrintDocFilter here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 *
 */
public class PrintDocFilter 
    extends BaseFilter
{
    public String description()
	{
	    return "Filter similar to the sink ca.gnewton.lusql.driver.file.PrintDocSink in that it prints out the documents fields/values. But being a filter subsequent sinks can still operate. Does not alter the document.";
	} 
    /**
     * Describe luSql here.
     */
    private LuSql luSql;

  /**
   * Describe properties here.
   */
  private Properties properties;
  public Doc filter(Doc doc)
      throws ca.gnewton.lusql.core.FatalFilterException
  {
      System.out.print("\tPrintDocFilter:START");
      Iterator<String>it = doc.getFieldNames();
      while(it.hasNext())
      {
	  String field = it.next();
	  System.out.print("\n\tPrintDocFilter:\t" 
			     + field);
	  List<String> values = doc.getFieldValues(field);
	  for(String value:values)
	      	  System.out.print("<<" 
				   + value
				   + ">>"
		      );
      }
      System.out.println("\n\tPrintDocFilter:END");
    return doc;
  }

    @Override
    public void done()
	throws ca.gnewton.lusql.core.PluginException
	{

	}


}
