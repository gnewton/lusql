package ca.gnewton.lusql.core;

import java.util.concurrent.*;

/**
 * Describe class AddDocumentExecutor here.
 *
 *
 * Created: Fri Sep 12 11:58:17 2008
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 */
public class AddDocumentExecutor 		
    extends ThreadPoolExecutor
{
public AddDocumentExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, 
			   BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler,
			   LuSql newLuSql
			   ) 
    	{
	    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
	    setLuSql(newLuSql);
	}

    /**
     * Describe luSql here.
     */
    private LuSql luSql;
    protected void afterExecute(Runnable r,
				Throwable t)
	{
	    super.afterExecute(r, t);
	    //luSql.returnAddDocument((AddDocument)r);
	}

    /**
     * Get the <code>LuSql</code> value.
     *
     * @return a <code>LuSql</code> value
     */
    public final LuSql getLuSql() {
	return luSql;
    }

    /**
     * Set the <code>LuSql</code> value.
     *
     * @param newLuSql The new LuSql value.
     */
    public final void setLuSql(final LuSql newLuSql) {
	this.luSql = newLuSql;
    }
}




