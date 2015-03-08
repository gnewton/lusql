package ca.gnewton.lusql.core;
import javax.sql.DataSource;
import org.apache.lucene.document.*;
import java.util.*;

/**
 * Describe interface DBDocFilter here.
 *
 *
 * Created: Fri Sep 12 13:09:29 2008
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
abstract public class DBDocFilterImp 
    extends NullFilter
    implements DBDocFilter
    
{

    /**
     * Describe dataSource here.
     */
    private DataSource dataSource;

    /**
     * Get the <code>DataSource</code> value.
     *
     * @return a <code>DataSource</code> value
     */
    public final DataSource getDataSource() {
	return dataSource;
    }

    /**
     * Set the <code>DataSource</code> value.
     *
     * @param newDataSource The new DataSource value.
*/
    public final void setDataSource(final DataSource newDataSource) {
	this.dataSource = newDataSource;
    }


}
