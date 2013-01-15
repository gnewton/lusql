package ca.gnewton.lusql.driver.jdbc; 

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import ca.gnewton.lusql.metadata.*;

import java.sql.*;
import java.util.*;
import java.lang.annotation.*;
import javax.sql.DataSource;

import org.apache.commons.dbcp.*;
import org.apache.commons.dbcp.cpdsadapter.*;
import org.apache.commons.dbcp.datasources.*;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.*;


/**
 * Describe class JDBCDataSource here.
 *
 *
 * Created: Thu Dec  4 15:23:30 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class JDBCDocSource 
	extends AbstractDocSource
	implements LuSqlFields
{
	//static Category cat = Category.getInstance(LuSql.class.getName());

	@Override
	public String description()
	{
		return "Source that gets records from JDBC";
	}

	private String query;
	private String driver;
	private Set<String>fields = null;
	private String dbUrl;
	//private int fetchSize = Integer.MIN_VALUE;
	private int fetchSize = 100;

	private String userid = null;
	private String password = null;

	@Override
	public Properties explainProperties()
	{
		Properties p = new Properties();
		p.setProperty(QueryKey, "SQL query to apply");
		p.setProperty(DBUrlKey, "JDBC connect URL");
		p.setProperty(JDBCDriverKey, "JDBC Driver class name");
		p.setProperty(JDBCFetchSizeKey, "JDBC fetch size");
		return p;
	}

	ObjectPool connectionPool = null;
	DataSource ds = null;
	//
	@Override
	public void init(MultiValueProp p) throws PluginException
	{
		extractProperties(p);
	    
		//cat.info("JDBCDocSource:driver=" + getDriver());
		//	    cat.info("JDBCDocSource:dburl=" + getDbUrl());
		//	    cat.info("JDBCDocSource:query=" + getQuery());
		//	    cat.info("JDBCDocSource:fetchSize=" + getFetchSize());
	    
		try
			{
				Class.forName(getDriver());
				Class.forName("org.apache.commons.dbcp.PoolingDriver");
				PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
				//cat.info("JDBCDocSource:driver version=" + driver.getMajorVersion() + "." + driver.getMinorVersion());
			}
		catch(SQLException t)
			{
				//cat.error("SQLException");
				t.printStackTrace();
				throw new PluginException();
			}
		catch(ClassNotFoundException t)
			{
				//cat.error("Class not found exception for: " + getDriver());
				t.printStackTrace();
				throw new PluginException("Problem with driver: " + getDriver() + " OR org.apache.commons.dbcp not in CLASSPATH");
			}
	    
		connectionPool = new GenericObjectPool(null);
	    
		// Expose validation query like 	    cProps.setProperty("validationQuery", "select id from Publisher where id=1");
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(getDbUrl(), null);
		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,connectionPool,null,null,false,true);
		poolableConnectionFactory.setDefaultReadOnly(true);
	    
		((GenericObjectPool)connectionPool).setMinIdle(2);
		((GenericObjectPool)connectionPool).setMaxActive(32);

		//cat.info("Getting DataSource");
		ds = getDataSource();
		try
			{
				conn = ds.getConnection(); 

				// hint for db optimization
				conn.setReadOnly(true);
				conn.setAutoCommit(false);
				conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
				// TODO
				//conn.setTransactionIsolation(getTransactionIsolation());
				//System.out.println("Transaction level=" + getTransactionIsolation());

				stmt = getStatement(conn);
				rs = getResults(stmt);
				md = rs.getMetaData();
				nFields = md.getColumnCount();
				fieldNames = new String[nFields];
				for(int i=0; i<nFields; i++)
					fieldNames[i] = md.getColumnName(i+1);
			}
		catch(SQLException t)
			{
				//cat.error("SQLException: query=\"" + getQuery() + "\"");
				t.printStackTrace();
				throw new PluginException("Major SQL problem");
			}
	}

	@Override
	public Doc next()  throws DataSourceException
	{
		Doc doc = null;
		try
			{
				if(!rs.next())
					doc = new DocImp().setLast(true);	
				else
					doc = makeDoc(rs);
			}
		catch(SQLException t)
			{
				done();
				t.printStackTrace();
				throw new DataSourceException("Major SQL problem");
			}
		return doc;
	}

	Connection conn = null;
	Statement stmt = null;
	ResultSet rs = null;
	ResultSetMetaData md = null;
	int nFields;
	String[] fieldNames;

	@Override    
	public void done()
	{
		//cat.info("Shutting down JDBC");
		try
			{
				//cat.info("Shutting down JDBC: resultset");
				//if(rs != null)
				//rs.close();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}

		try
			{
				//cat.info("Shutting down JDBC: statement");
				//if(stmt != null)
				//stmt.close();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}
		try
			{
				//cat.info("Shutting down JDBC: connection");
				//if(conn != null)
				//conn.close();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}

	}

	Doc makeDoc(ResultSet thisRs)
		throws DataSourceException
	{
		Doc d = new DocImp();
		//Doc d = LuSql.newDoc();
		try
			{
				populate(d, rs, fieldNames);
			}
		catch(SQLException t)
			{
				t.printStackTrace();
				throw new DataSourceException();
			}
		return d;
	}

	public void populate(Doc d, ResultSet rs, String[] fieldNames) 
		throws SQLException
	{
		int nFields = fieldNames.length;
		for(int i=1; i<=nFields; i++)
			{
				if(fields != null && !fields.contains(fieldNames[i-1]))
					continue;
				d.addField(fieldNames[i-1], rs.getString(i));
			}
	}


	Connection getConnection()
		throws SQLException
	{
		//Class.forName(getDBDriverName());
		//DriverManger.setLoginTimeout(10);
		//Connection conn = DriverManager.getConnection(getDBUrl());
		//getUserID(),
		//getUserPassword());

		Connection thisConn = ds.getConnection();
	    

		// hint for db optimization
		thisConn.setReadOnly(true);
		thisConn.setAutoCommit(false);
		thisConn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
	    
		//TRANSACTION_NONE not supported by MySQL
		//if(isMySql())
		thisConn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		//else
		//conn.setTransactionIsolation(Connection.TRANSACTION_NONE);

		return thisConn;
	}

	Statement getStatement(Connection conn)
		throws SQLException
	{
		Statement thisStmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
		                                          java.sql.ResultSet.CONCUR_READ_ONLY);

		thisStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
		thisStmt.setFetchSize(getFetchSize());
	    
		return thisStmt;
	}


	ResultSet getResults(Statement stmt)
		throws java.sql.SQLException
     
	{
		ResultSet thisRs = null;
		thisRs = stmt.executeQuery(getQuery());
		return thisRs;
	}

	/**
	 * Get the <code>Query</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getQuery() {
		return query;
	}

	/**
	 * Set the <code>Query</code> value.
	 *
	 * @param newQuery The new Query value.
	 */
    

	//@LuSqlParameter(description="SQL query to apply")

	public final void setQuery(final String newQuery) {
		this.query = newQuery;
	}

	/**
	 * Get the <code>Driver</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getDriver() {
		return driver;
	}

	/**
	 * Set the <code>Driver</code> value.
	 *
	 * @param newDriver The new Driver value.
	 */
	public final void setDriver(final String newDriver) {
		this.driver = newDriver;
	}

	/**
	 * Get the <code>DbUrl</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getDbUrl() {
		return dbUrl;
	}

	/**
	 * Set the <code>DbUrl</code> value.
	 *
	 * @param newDbUrl The new DbUrl value.
	 */
	public final void setDbUrl(final String newDbUrl) {
		this.dbUrl = newDbUrl;
	}

	/**
	 * Get the <code>FetchSize</code> value.
	 *
	 * @return an <code>int</code> value
	 */
	public final int getFetchSize() {
		return fetchSize;
	}

	/**
	 * Set the <code>FetchSize</code> value.
	 *
	 * @param newFetchSize The new FetchSize value.
	 */
	public final void setFetchSize(final int newFetchSize) {
		this.fetchSize = newFetchSize;
	}
	void extractProperties(MultiValueProp p)
		throws PluginException
	{
		if(!p.containsKey(QueryKey))
			{
				throw new PluginException("Missing query");
			}
		setQuery(p.getProperty(QueryKey).get(0));
	    
		if(!p.containsKey(DBUrlKey))
			throw new PluginException("Missing JDBC Connection URL");
		setDbUrl(p.getProperty(DBUrlKey).get(0));

		if(!p.containsKey(JDBCDriverKey))
			throw new PluginException("Missing JDBC Driver");
		setDriver(p.getProperty(JDBCDriverKey).get(0));

		if(p.containsKey(JDBCFetchSizeKey))
			{
				int fs;
				try
					{
						fs = Integer.parseInt(p.getProperty(JDBCFetchSizeKey).get(0));
					}
				catch(NumberFormatException t)
					{
						throw new PluginException("Fetch Size not integer: " + p.getProperty(JDBCFetchSizeKey).get(0));
					}
				if(fs <= 0)
					setFetchSize(Integer.MIN_VALUE);
				else
					setFetchSize(fs);
		    
			}
	}



	/**
	 * Describe <code>main</code> method here.
	 *
	 * @param args a <code>String</code> value
	 */
	public static final void main(final String[] args) 
	{
		DocSource ds = new JDBCDocSource();
		MultiValueProp p = new MultiValueProp();
		p.setProperty(QueryKey, "select * from Article limit 100");
		p.setProperty(DBUrlKey, "jdbc:mysql://blue01/dartejos?user=gnewton&password=GNewton&autoReconnectForPools=true&cacheResultSetMetadata=true");
		p.setProperty(JDBCDriverKey, "com.mysql.jdbc.Driver");
		p.setProperty(JDBCFetchSizeKey,"1");

		try
			{
				ds.init(p);
			}
		catch(PluginException t)
			{
				t.printStackTrace();
			}
	
		Doc d = null;
		try
			{
				while((d = ds.next()) != null)
					{
						System.out.println(d);
					}
				ds.done();
			}
		catch(DataSourceException t)
			{
				t.printStackTrace();
			}
		catch(PluginException t)
			{
				t.printStackTrace();
			}
	}

	public DataSource getDataSource()
	{
		if(ds == null)
			ds = new PoolingDataSource(connectionPool);
		return ds;
	}

	public void addField(final String field)
	{
		if(fields == null)
			fields = new HashSet<String>();
		fields.add(field);
	}

	/**
	 * Get the <code>Userid</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getUserid() {
		return userid;
	}

	/**
	 * Set the <code>Userid</code> value.
	 *
	 * @param newUserid The new Userid value.
	 */
	public final void setUserid(final String newUserid) {
		this.userid = newUserid;
	}

	/**
	 * Get the <code>Password</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getPassword() {
		return password;
	}

	/**
	 * Set the <code>Password</code> value.
	 *
	 * @param newPassword The new Password value.
	 */
	public final void setPassword(final String newPassword) {
		this.password = newPassword;
	}


}
