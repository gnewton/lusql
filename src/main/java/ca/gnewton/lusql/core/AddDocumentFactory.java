package ca.gnewton.lusql.core;
import java.util.*;
import org.apache.commons.pool.BasePoolableObjectFactory; 
import org.apache.lucene.document.*;

/**
 * Describe class AddDocumentFactory here.
 *
 *
 * Created: Thu Sep 11 18:18:11 2008
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a>   
 * @copyright CISTI / National Research Council Canada, , Glen Newton
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 */
public class AddDocumentFactory 
    extends BasePoolableObjectFactory
{
    int count = 0;
    int returned = 0;
    public Object makeObject() { 
	++count;
	return new AddDocument(); 

    }

    public void passivateObject(Object obj) { 
	++returned;
	//if(returned%1000 == 0)
	//System.err.println("AddDocumentFactory returned: " + returned);
	AddDocument ad = (AddDocument)obj; 
	ad.setDocs(null);
	AddDocument.setDocSink(null);
	AddDocument.setFilter(null);
	ad.setLuSql(null);
    } 
}
