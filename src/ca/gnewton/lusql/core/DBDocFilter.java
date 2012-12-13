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
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public abstract interface DBDocFilter 
{

    public DataSource getDataSource();
    public void setDataSource(final DataSource newDataSource);
}
