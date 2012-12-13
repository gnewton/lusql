package ca.gnewton.lusql.core;

import org.apache.lucene.document.*;
import java.util.*;
import java.sql.*;
import javax.sql.DataSource;
import ca.gnewton.lusql.util.*;
import java.util.concurrent.locks.ReentrantLock;
/**
 * Describe class SubQueryJoinFilter here.

 -K "|"

X = "id|NNN NNN|select name,city from Article where id = $1"
or
X = "id|select name,city from Article where id = $1"
or
X = "id|NNN|select name,city from Article where id = $1"


 *
 *
 * Created: Fri Oct 24 14:30:47 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class SubQueryJoinFilter 
    extends DBDocFilterImp
{
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * Describe subQuery here.
     */
    private SubQuery subQuery;

    /**
     * Describe cached here.
     */
    private boolean cached = false;

    Map<String, List<Map<String,String>>> cache = new WeakHashMap<String, List<Map<String,String>>>();
    //Map<String, List<Map<String,String>>> cache = new HashMap<String, List<Map<String,String>>>();

    /**
     * Describe fieldIndexParameters here.
     */
    private Map<String, LuceneFieldParameters> fieldIndexParameters;

    public SubQueryJoinFilter(final SubQuery newSubQuery) 
	{
	    setSubQuery(newSubQuery);
	    setCached(newSubQuery.isCached());
	    
	}

    LuceneFieldParameters[] paras = null;
    int hit = 0;
    int miss=0;

    public Doc filter(Doc doc)
	throws ca.gnewton.lusql.core.FatalFilterException
	{
	    MultiValueProp p = getProperties();
	    
	    Connection conn = null;
	    Statement stmt = null;
	    ResultSet rs = null;

	    try
	    {
		String query = makeQuery(doc);
		if(isCached())
		{
		    if(isCached())
		    {
			boolean inCache = false;
			List<Map<String,String>>fields = cache.get(query);
			lock.lock();  // block until condition holds
			try 
			{
			    inCache = cache.containsKey(query);
			    if(inCache)
				fields = cache.get(query);
			} 
			finally 
			{
			    lock.unlock();
			}
			
			if(inCache)
			{
			    ++hit;
			    if(hit%10000 == 0)
				System.out.println("Subquery: " + getSubQuery().getQuery() + "   cache hits=" + hit 
						   + "  misses=" + miss
						   + "  rate=" + ((float)hit)/(float)((hit+miss))
						   + "  cache size=" + cache.size());
			    
			    
			    if(fields != null)
			    {
				//System.out.println("Cache hit********************************************");
				//System.out.println(query);
				
				return getFromCache(doc, fields);
			    }
			}
			++miss;
		    }
		 
		}
		conn = getDataSource().getConnection();
		conn.setReadOnly(true);
		conn.setAutoCommit(false);
		conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		
		stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					    java.sql.ResultSet.CONCUR_READ_ONLY);
		
		if(p.get(LuSqlFields.IsMysqlKey).equals("true"))
		    stmt.setFetchSize(Integer.MIN_VALUE);
		else
		    stmt.setFetchSize(50);
		
		rs = null;
		
		// Query 
		

		//System.out.println("Subquery=" + query);
		try
		{
		    //rs = new ReadOnlyResultSet(stmt, query);
		    rs = stmt.executeQuery(query);
		}
		catch(Throwable t1)
		{
		    System.out.println("Exception thrown using SQL=["
				       + query
				       + "]");
		    throw t1;
		}
		
		ResultSetMetaData md = rs.getMetaData();
		int nFields = md.getColumnCount();
		String[] fieldNames = new String[nFields];
		for(int i=0; i<nFields; i++)
		    fieldNames[i] = md.getColumnName(i+1);

		List<Map<String,String>> fieldMaps = null;
		Map<String,String> fieldMap = null;
		if(isCached())
		    {
			fieldMap = new HashMap<String,String>();
			fieldMaps = new ArrayList<Map<String,String>>();
		    }
		LuSql lusql = getLuSql();
		// In realuty
		int count = 0;
		while(rs.next())
		{
		    String s = new String();
		    Set<String>ff = new HashSet<String>();
		    for(int i=1; i<=nFields; i++)
		    {
			String fieldName = fieldNames[i-1];
			String value = rs.getString(i);
			doc.addField(fieldName, value);
			doc.addFieldParameter(fieldName, lusql.getLuceneFieldParameters(fieldName));
			if(isCached() && !ff.contains(value) && !fieldMap.containsKey(fieldName))
			    {
				fieldMap.put(fieldName, value);
				ff.add(value);
			    }
		    }
		    count++;
		    if(isCached())
		    {
			lock.lock();  
			try
			{
			    if(!fieldMaps.contains(fieldMap))
				fieldMaps.add(fieldMap);
			}
			finally 
			{
			    lock.unlock();
			}
		    }
		}
		if(isCached())
		{
		    lock.lock();  
		    try
		    {
			if(!cache.containsKey(query))
			    cache.put(query, fieldMaps);
		    }
		    finally 
		    {
			lock.unlock();
		    }
		}
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		return null;
	    }
	    finally
	    {
		try
		{
		    conn.close();
		    stmt.close();
		    rs.close();
		}
		catch(Throwable t)
		{
		    //OK
		}
	    }
	    return doc;
	}
    
    /**
     * Get the <code>SubQuery</code> value.
     *
     * @return a <code>SubQuery</code> value
     */
    public final SubQuery getSubQuery() {
	return subQuery;
    }

    /**
     * Set the <code>SubQuery</code> value.
     *
     * @param newSubQuery The new SubQuery value.
     */
    public final void setSubQuery(final SubQuery newSubQuery) {
	this.subQuery = newSubQuery;
    }

    String makeQuery(Doc doc)
	throws NullPointerException
	{
	    
	    if(doc == null)
		System.err.println("SubQueryJoinFilter error: makeQuery Doc is null");
	    if(getSubQuery() == null)
		System.err.println("SubQueryJoinFilter error: makeQuery getSubQuery() is null");

	    if(!doc.containsField(getSubQuery().getDocField()))
	    {
		System.err.println("Fatal error: The key field ["
				   + getSubQuery().getDocField()
				   + "] in your subquery does not match any field is your main SQL query");
		throw new NullPointerException();
	    }
	    String keyValue = doc.getFieldValues(getSubQuery().getDocField()).get(0);
	    return getSubQuery().makeQuery(keyValue);

	}

    /**
     * Get the <code>Cached</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isCached() {
	return cached;
    }

    /**
     * Set the <code>Cached</code> value.
     *
     * @param newCached The new Cached value.
     */
    public final void setCached(final boolean newCached) {
	this.cached = newCached;
    }

    synchronized Doc getFromCache(Doc doc, List<Map<String,String>>records)
	{
	    LuSql lusql = getLuSql(); 
	    Iterator<Map<String,String>> it = records.iterator();
	    while(it.hasNext())
	    {
		Map<String,String>record = it.next();
		Iterator<String> it2 = record.keySet().iterator();
		while(it2.hasNext())
		{
		    String fieldName = it2.next();
		    String value = record.get(fieldName);
		    doc.addField(fieldName, value);
		    doc.addFieldParameter(fieldName, lusql.getLuceneFieldParameters(fieldName));
		}
	    }
	    return doc;
	}

    public void onDone()
	{
	    if(isCached())
		System.out.println("Subquery: " + getSubQuery().getQuery() + "   cache hits=" + hit 
				   + "  misses=" + miss
				   + "  rate=" + ((float)hit)/(float)((hit+miss)));
	}
}
