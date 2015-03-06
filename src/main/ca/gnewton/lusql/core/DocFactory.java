package ca.gnewton.lusql.core;
import java.util.*;
import org.apache.commons.pool.BasePoolableObjectFactory; 
import org.apache.lucene.document.*;

/**
 * Describe class DocFactory here.
 *
 *
 * Created: Thu Sep 11 18:18:11 2008
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public class DocFactory 
    extends BasePoolableObjectFactory
{
    int count = 0;
    int returned = 0;
    public Object makeObject() { 
	++count;
	//if(count%1000 == 0)
	//System.out.println("DocFactory created: " + count);
	return new DocImp(); 

    }

    public void passivateObject(Object obj) { 
	++returned;
	//if(returned%1000 == 0)
	//System.out.println("DocFactory returned: " + returned);
        Doc doc = (Doc)obj; 
	doc.clear();
    } 
}
