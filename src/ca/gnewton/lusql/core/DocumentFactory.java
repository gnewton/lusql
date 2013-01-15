package ca.gnewton.lusql.core;
import java.util.*;
import org.apache.commons.pool.BasePoolableObjectFactory; 
import org.apache.lucene.document.*;

/**
 * Describe class DocumentFactory here.
 *
 *
 * Created: Thu Sep 11 18:18:11 2008
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public class DocumentFactory 
    extends BasePoolableObjectFactory
{
    int count = 0;
    int returned = 0;
    public Object makeObject() { 
	++count;
	//if(count%1000 == 0)
	//System.out.println("DocumentFactory created: " + count);
	return new Document(); 

    }

    public void passivateObject(Object obj) { 
	++returned;
	//if(returned%1000 == 0)
	//System.out.println("DocumentFactory returned: " + returned);
        Document doc = (Document)obj; 
	List<Fieldable> fields = doc.getFields();
	Iterator<Fieldable> it = fields.iterator();
	while(it.hasNext())
	{
	    doc.removeField(((Field)it.next()).name());
	}
    } 
}
