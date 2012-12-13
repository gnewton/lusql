package ca.gnewton.lusql.core;
import org.apache.lucene.document.*;
import java.util.Properties;

/**
 * Describe class NullFilter here.
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
public class NullFilter 
    extends BaseFilter
{
    public String showState(int n)
    {
	return 
	    ca.gnewton.lusql.util.Util.offset("NullFilter\n", n)
	    + ca.gnewton.lusql.util.Util.offset("No values", n+1)
						 ;
    }


    

    public Doc filter(Doc doc)
	throws ca.gnewton.lusql.core.FatalFilterException
    {
	return doc;
    }

    public String description()
	{
	    return "Filter that does nothing";
	}
}
