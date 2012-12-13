package ca.gnewton.lusql.core;

import java.sql.*;
import java.io.*;
import java.lang.Number.*;
import java.math.*;
import java.util.*;
import java.util.zip.*;
import java.net.*;


/**
 * Describe class ReadOnlyResultSet here.
 *
 *
 * Created: Tue Feb 12 13:29:33 2008
 *
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 */

public class ReadOnlyResultSet 
    implements ResultSet, Serializable
{
    static int DefaultChunkSize = 5000;
    private int rowCount = 0;


    /**
     * Describe resultSet here.
     */
    private ResultSet resultSet = null;

    /**
     * Describe query here.
     */
    private String query;

    /**
     * Describe statement here.
     */
    private Statement statement;

    /**
     * Describe offset here.
     */
    private int offset=1;

    /**
     * Describe chunkSize here.
     */
    private int chunkSize=100;

    /**
     * Describe SQLStatement here.
     */
    private Statement SQLStatement;

    /**
     * Describe verbose here.
     */
    private boolean verbose = false;


    
    public ReadOnlyResultSet(Statement newStatement, String newQuery)
	throws SQLException
	{
	    this(newStatement, newQuery, DefaultChunkSize);
	    //return new ReadOnlyResultSet(newStatement, newQuery, chunkSize);
	}

    public ReadOnlyResultSet(Statement newStatement, String newQuery, int newChunkSize)
	throws SQLException
	{
	    setChunkSize(newChunkSize);
	    setSQLStatement(newStatement);
	    setQuery(newQuery);
	    
	    runQuery();
	}

    public int getRowCount()
	{
	    return rowCount;
	}

    //ObjectOutputStream oout2 = null;
    public void runQuery()
	throws SQLException
	{
	    StringBuilder q = new StringBuilder(getQuery());
	    if(chunkSize > 0)
		q.append(" limit " + offset + ", " + chunkSize);
	    
	    long t0 = System.currentTimeMillis();
	    resultSet = SQLStatement.executeQuery(q.toString());
	    /*
	    try
	    {
		if(oout2 == null)
		{
		    System.out.println("Opening resultset.bin");
		    oout2 = new ObjectOutputStream(
			new GZIPOutputStream(
			    new FileOutputStream("resultset.bin")));
		}
		if(oout2 != null)
		{
		    oout2.writeObject(this);
		}
		oout2.flush();
		oout2.close();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	    
	    */
	    if(isVerbose())
	    {
		System.out.println("************ Query time=" + ((System.currentTimeMillis()-t0)/1000)
				   + "s");
		System.out.println("ReadOnlyResultSet q=o" + q);
	    }
				   
	    offset += chunkSize;
	    //System.out.println("ReadOnlyResultSet chunkSize=" + chunkSize);
	    //System.out.println("ReadOnlyResultSet offset=" + offset);
	}

    /**
     * Get the <code>SQLStatement</code> value.
     *
     * @return a <code>Statement</code> value
     */
    public final Statement getSQLStatement() {
	return SQLStatement;
    }

    /**
     * Set the <code>GetSQLStatement</code> value.
     *
     * @param newSQLStatement The new GetSQLStatement value.
     */
    public final void setSQLStatement(final Statement newSQLStatement) {
	this.SQLStatement = newSQLStatement;
    }
    /**
     * Get the <code>Offset</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getOffset() {
	return offset;
    }

    /**
     * Set the <code>Offset</code> value.
     *
     * @param newOffset The new Offset value.
     */
    public final void setOffset(final int newOffset) {
	this.offset = newOffset;
    }

    /**
     * Get the <code>ChunkSize</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getChunkSize() {
	return chunkSize;
    }

    /**
     * Set the <code>ChunkSize</code> value.
     *
     * @param newChunkSize The new ChunkSize value.
     */
    public final void setChunkSize(final int newChunkSize) {
	this.chunkSize = newChunkSize;
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
    public final void setQuery(final String newQuery) {
	this.query = newQuery;
    } 
    /**
     * Get the <code>ResultSet</code> value.
     *
     * @return a <code>ResultSet</code> value
     */
    public final ResultSet getResultSet() {
	return resultSet;
    }

    /**
     * Set the <code>ResultSet</code> value.
     *
     * @param newResultSet The new ResultSet value.
     */
    public final void setResultSet(final ResultSet newResultSet) {
	this.resultSet = newResultSet;
    }


    //////////////// As per ResultSet API

    public boolean absolute(int row)
	{
	    return false;
	}

    public void 	afterLast()
	{
	    
	}

    public void 	beforeFirst()
	{

	}

    public void 	cancelRowUpdates()
	{

	}

    public void 	clearWarnings()
     throws SQLException
	{
	    resultSet.clearWarnings();
	}

    public void 	close()
     throws SQLException
	{
	    resultSet.close();
	}

    public void 	deleteRow()
	{

	}

    public int 	findColumn(String columnName)
     throws SQLException
	{
	    return resultSet.findColumn(columnName);
	}


    public boolean 	first()
     throws SQLException
	{
	    return resultSet.first();
	}

    public Array 	getArray(int i)
     throws SQLException
	{
	    return resultSet.getArray(i);
	}

    public Array 	getArray(String colName)
     throws SQLException
	{
	    return resultSet.getArray(colName);
	}

    public InputStream getAsciiStream(int columnIndex)
	throws SQLException
	{
	    return resultSet.getAsciiStream(columnIndex);
	}


    public InputStream 	getAsciiStream(String columnName)
     throws SQLException
	{
	    return resultSet.getAsciiStream(columnName);
	}

    public BigDecimal 	getBigDecimal(int columnIndex)
     throws SQLException
	{
	    return resultSet.getBigDecimal(columnIndex);
	}
    
/*
  public BigDecimal 	getBigDecimal(int columnIndex, int scale)
     throws SQLException
	{
	    return resultSet.getBigDecimal(columnIndex, scale);
	}
*/

    public BigDecimal 	getBigDecimal(String columnName)
     throws SQLException
	{
	    return resultSet.getBigDecimal(columnName);
	}

    /*
      public BigDecimal 	getBigDecimal(String columnName, int scale)
     throws SQLException
	{
	    return resultSet.getBigDecimal(columnName, scale);
	}
    */

    public InputStream 	getBinaryStream(int columnIndex)
     throws SQLException
	{
	    return resultSet.getBinaryStream(columnIndex);
	}

    public InputStream 	getBinaryStream(String columnName)
     throws SQLException
	{
	    return resultSet.getBinaryStream(columnName);
	}

    public Blob 	getBlob(int i)
     throws SQLException
	{
	    return resultSet.getBlob(i);
	}

    public Blob 	getBlob(String colName)
     throws SQLException
	{
	    return resultSet.getBlob(colName);
	}

    public boolean 	getBoolean(int columnIndex)
     throws SQLException
	{
	    return resultSet.getBoolean(columnIndex);
	}

    public boolean 	getBoolean(String columnName)
     throws SQLException
	{
	    return resultSet.getBoolean(columnName);
	}

    public byte 	getByte(int columnIndex)
     throws SQLException
	{
	    return resultSet.getByte(columnIndex);
	}

    public byte 	getByte(String columnName)
     throws SQLException
	{
	    return resultSet.getByte(columnName);
	}

    public byte[] 	getBytes(int columnIndex)
     throws SQLException
	{
	    return resultSet.getBytes(columnIndex);
	}

    public byte[] 	getBytes(String columnName)
     throws SQLException
	{
	    return resultSet.getBytes(columnName);
	}

    public Reader 	getCharacterStream(int columnIndex)
     throws SQLException
	{
	    return resultSet.getCharacterStream(columnIndex);
	}

    public Reader 	getCharacterStream(String columnName)
     throws SQLException
	{
	    return resultSet.getCharacterStream(columnName);
	}

    public Clob getClob(int i)
     throws SQLException
	{
	    return resultSet.getClob(i);
	}

    public Clob getClob(String colName)
     throws SQLException
	{
	    return resultSet.getClob(colName);
	}

    public int getConcurrency()
     throws SQLException
	{
	    return resultSet.getConcurrency();
	}

    public String getCursorName()
     throws SQLException
	{
	    return resultSet.getCursorName();
	}

    public java.sql.Date getDate(int columnIndex)
     throws SQLException
	{
	    return resultSet.getDate(columnIndex);
	}

    public java.sql.Date getDate(int columnIndex, Calendar cal)
     throws SQLException
	{
	    return resultSet.getDate(columnIndex, cal);
	}

    public java.sql.Date getDate(String columnName)
     throws SQLException
	{
	    return resultSet.getDate(columnName);
	}

    public java.sql.Date getDate(String columnName, Calendar cal)
     throws SQLException
	{
	    return resultSet.getDate(columnName, cal);
	}

    public double getDouble(int columnIndex)
     throws SQLException
	{
	    return resultSet.getDouble(columnIndex);
	}

    public double getDouble(String columnName)
     throws SQLException
	{
	    return resultSet.getDouble(columnName);
	}

    public int getFetchDirection()
     throws SQLException
	{
	    return resultSet.getFetchDirection();
	}

    public int getFetchSize()
     throws SQLException
	{
	    return resultSet.getFetchSize();
	}

    public float getFloat(int columnIndex)
     throws SQLException
	{
	    return resultSet.getFloat(columnIndex);
	}

    public float getFloat(String columnName)
     throws SQLException
	{
	    return resultSet.getFloat(columnName);
	}

    public int getInt(int columnIndex)
     throws SQLException
	{
	    return resultSet.getInt(columnIndex);
	}

    public int getInt(String columnName)
     throws SQLException
	{
	    return resultSet.getInt(columnName);
	}

    public long getLong(int columnIndex)
     throws SQLException
	{
	    return resultSet.getLong(columnIndex);
	}

    public long getLong(String columnName)
     throws SQLException
	{
	    return resultSet.getLong(columnName);
	}

    public ResultSetMetaData getMetaData()
     throws SQLException
	{
	    return resultSet.getMetaData();
	}

    public Object getObject(int columnIndex)
     throws SQLException
	{
	    return resultSet.getObject(columnIndex);
	}

    public Object getObject(int i, Map map)
     throws SQLException
	{
	    return resultSet.getObject(i, map);
	}

    public Object getObject(String columnName)
     throws SQLException
	{
	    return resultSet.getObject(columnName);
	}

    public Object getObject(String colName, Map map)
     throws SQLException
	{
	    return resultSet.getObject(colName, map);
	}

    public Ref getRef(int i)
	throws SQLException
	{
	    return resultSet.getRef(i);
	}

    public Ref getRef(String colName)
	throws SQLException
	{
	    return resultSet.getRef(colName);
	}

    public int getRow()
	throws SQLException
	{
	    return resultSet.getRow();
	}

    public short getShort(int columnIndex)
	throws SQLException
	{
	    return resultSet.getShort(columnIndex);
	}

    public short getShort(String columnName)
	throws SQLException
	{
	    return resultSet.getShort(columnName);
	}

    public Statement getStatement()
	throws SQLException
	{
	    return resultSet.getStatement();
	}

    public String getString(int columnIndex)
	throws SQLException
	{
	    return resultSet.getString(columnIndex);
	}

    public String getString(String columnName)
	throws SQLException
	{
	    return resultSet.getString(columnName);
	}

    public Time getTime(int columnIndex)
	throws SQLException
	{
	    return resultSet.getTime(columnIndex);
	}

    public Time getTime(int columnIndex, Calendar cal)
	throws SQLException
	{
	    return resultSet.getTime(columnIndex, cal);
	}

    public Time getTime(String columnName)
	throws SQLException
	{
	    return resultSet.getTime(columnName);
	}

    public Time getTime(String columnName, Calendar cal)
	throws SQLException
	{
	    return resultSet.getTime(columnName, cal);
	}

    public Timestamp getTimestamp(int columnIndex)
	throws SQLException
	{
	    return resultSet.getTimestamp(columnIndex);
	}

    public Timestamp getTimestamp(int columnIndex, Calendar cal)
	throws SQLException
	{
	    return resultSet.getTimestamp(columnIndex, cal);
	}

    public Timestamp getTimestamp(String columnName)
	throws SQLException
	{
	    return resultSet.getTimestamp(columnName);
	}

    public Timestamp getTimestamp(String columnName, Calendar cal)
	throws SQLException
	{
	    return resultSet.getTimestamp(columnName, cal);
	}

    public int getType()
	throws SQLException
	{
	    return resultSet.getType();
	}

    @Deprecated
      public InputStream getUnicodeStream(int columnIndex)
	throws SQLException
	{
	    return resultSet.getUnicodeStream(columnIndex);
	}

    @Deprecated
    public InputStream getUnicodeStream(String columnName)
	throws SQLException
	{
	    return resultSet.getUnicodeStream(columnName);
	}


    public SQLWarning getWarnings()
	throws SQLException
	{
	    return resultSet.getWarnings();
	}

    public void insertRow()
	{

	}

    public boolean isAfterLast()
	throws SQLException
	{
	    return resultSet.isAfterLast();
	}

    public boolean isBeforeFirst()
	throws SQLException
	{
	    return resultSet.isBeforeFirst();
	}

    public boolean isFirst()
	throws SQLException
	{
	    return resultSet.isFirst();
	}

    public boolean isLast()
	throws SQLException
	{
	    return resultSet.isLast();
	}

    public boolean last()
	throws SQLException
	{
	    return resultSet.last();
	}

    public void 	moveToCurrentRow()
	{

	}

    public void 	moveToInsertRow()
	{

	}


    public boolean next()
	throws SQLException
	{
	    try
	    {

		if(resultSet == null)
		    runQuery();
		else
		    if(resultSet.isAfterLast())
			runQuery();
		
		if(resultSet == null)
		    return false;
		//System.out.println("rowCount=" + rowCount + "  isAfterLast=" + resultSet.isAfterLast());
		boolean next;
		
		next = resultSet.next();

		if(resultSet.isAfterLast())
		{
		    runQuery();
		    next = resultSet.next();
		}
		if(next)
		    ++rowCount;
	    //System.out.println("rowCount=" + rowCount + "  next=" + next + "  isAfterLast=" + resultSet.isAfterLast());
		return next;
	    }
	    catch(SQLException s)
	    {
		System.err.println("rowCount = " + rowCount);
		throw s;
	    }
	}

    public boolean previous()
	throws SQLException
	{
	    return resultSet.previous();
	}

    public void refreshRow()
	throws SQLException
	{
	    resultSet.refreshRow();
	}

    public boolean relative(int rows)
	throws SQLException
	{
	    return resultSet.relative(rows);
	}

    public boolean 	rowDeleted()
	{
	    return false;
	}

    public boolean 	rowInserted()
	{
	    return false;
	}

    public boolean 	rowUpdated()
	{
	    return false;
	}

    public void 	setFetchDirection(int direction)
	{

	}

    public void 	setFetchSize(int rows)
	{

	}

    public void 	updateAsciiStream(int columnIndex, InputStream x, int length)
	{

	}

    public void 	updateAsciiStream(String columnName, InputStream x, int length)
	{

	}

    public void 	updateBigDecimal(int columnIndex, BigDecimal x)
	{

	}

    public void 	updateBigDecimal(String columnName, BigDecimal x)
	{

	}

    public void 	updateBinaryStream(int columnIndex, InputStream x, int length)
	{

	}

    public void 	updateBinaryStream(String columnName, InputStream x, int length)
	{

	}

    public void 	updateBoolean(int columnIndex, boolean x)
	{

	}

    public void 	updateBoolean(String columnName, boolean x)
	{

	}

    public void 	updateByte(int columnIndex, byte x)
	{

	}

    public void 	updateByte(String columnName, byte x)
	{

	}

    public void 	updateBytes(int columnIndex, byte[] x)
	{

	}

    public void 	updateBytes(String columnName, byte[] x)
	{

	}

    public void 	updateCharacterStream(int columnIndex, Reader x, int length)
	{

	}

    public void 	updateCharacterStream(String columnName, Reader reader, int length)
	{

	}

    public void 	updateDate(int columnIndex, java.sql.Date x)
	{

	}

    public void 	updateDate(String columnName, java.sql.Date x)
	{

	}

    public void 	updateDouble(int columnIndex, double x)
	{

	}

    public void 	updateDouble(String columnName, double x)
	{

	}

    public void 	updateFloat(int columnIndex, float x)
	{

	}

    public void 	updateFloat(String columnName, float x)
	{

	}

    public void 	updateInt(int columnIndex, int x)
	{

	}

    public void 	updateInt(String columnName, int x)
	{

	}

    public void 	updateLong(int columnIndex, long x)
	{

	}

    public void 	updateLong(String columnName, long x)
	{

	}

    public void 	updateNull(int columnIndex)
	{

	}

    public void 	updateNull(String columnName)
	{

	}

    public void 	updateObject(int columnIndex, Object x)
	{

	}

    public void 	updateObject(int columnIndex, Object x, int scale)
	{

	}

    public void 	updateObject(String columnName, Object x)
	{

	}

    public void 	updateObject(String columnName, Object x, int scale)
	{

	}

    public void 	updateRow()
	{

	}

    public void 	updateShort(int columnIndex, short x)
	{

	}

    public void 	updateShort(String columnName, short x)
	{

	}

    public void 	updateString(int columnIndex, String x)
	{

	}

    public void 	updateString(String columnName, String x)
	{

	}

    public void 	updateTime(int columnIndex, Time x)
	{

	}

    public void 	updateTime(String columnName, Time x)
	{

	}

    public void 	updateTimestamp(int columnIndex, Timestamp x)
	{

	}

    public void 	updateTimestamp(String columnName, Timestamp x)
	{

	}

    public boolean wasNull()
	throws SQLException
	{
	    return resultSet.wasNull();
	}

    ///// 1.6

    public void updateNClob(int columnIndex, NClob nClob) 
	{

	}

    public void updateNClob(int columnIndex, Reader reader)
	{

	}
    public void updateNClob(int columnIndex, Reader reader, long length)
	{

	}
    public void updateNClob(String columnLabel, NClob nClob)
	{

	}
    public void updateNClob(String columnLabel, Reader reader)
	{

	}
    public void updateNClob(String columnLabel, Reader reader, long length)
	{

	}
    public void updateNString(int columnIndex, String nString)
	{

	}
    public void	pdateNString(String columnLabel, String nString)
	{

	}
    public void pdateNull(String columnLabel)
	{

	}

  

    public void updateClob(int columnIndex, Reader reader)
    {

    }

    public void updateClob(int columnIndex, Reader reader, long length)
	{
	    
	}

    public void updateClob(String columnLabel, Clob x)
    {

    }

    public void updateClob(String columnLabel, Reader reader)
    {

    }


    public void updateClob(String columnLabel, Reader reader, long length)
    {

    }

    public void 	updateBlob(int columnIndex, Blob x)
	{

	}

    public void 	updateBlob(int columnIndex, InputStream inputStream)
	{

	}

    public void 	updateBlob(int columnIndex, InputStream inputStream, long length)
	{

	}

    public void 	updateBlob(String columnLabel, Blob x)
	{

	}

    public void 	updateBlob(String columnLabel, InputStream inputStream)
	{

	}

    public void 	updateBlob(String columnLabel, InputStream inputStream, long length)
	{

	}

    public void 	updateCharacterStream(int columnIndex, Reader x)
	{

	}


    public void 	updateCharacterStream(int columnIndex, Reader x, long length)
	{

	}

    public void 	updateCharacterStream(String columnLabel, Reader reader)
	{

	}

    public void 	updateCharacterStream(String columnLabel, Reader reader, long length) 
	{

	}

    public void 	updateBinaryStream(int columnIndex, InputStream x)
	{

	}


    public void 	updateBinaryStream(int columnIndex, InputStream x, long length)
	{

	}

    public void 	updateBinaryStream(String columnLabel, InputStream x)
	{

	}

    public void 	updateAsciiStream(int columnIndex, InputStream x)
	{

	}



    public void 	updateAsciiStream(int columnIndex, InputStream x, long length)
	{

	}

    public void 	updateAsciiStream(String columnLabel, InputStream x)
	{

	}

    public void 	updateNCharacterStream(int columnIndex, Reader x)
	{

	}

    public void 	updateNCharacterStream(int columnIndex, Reader x, long length)
	{

	}

    public void 	updateNCharacterStream(String columnLabel, Reader reader)
	{

	}

    public void 	updateNCharacterStream(String columnLabel, Reader reader, long length) 
	{

	}


    public void updateAsciiStream(String columnLabel, InputStream x, long length)
	{

	}




    public void 	updateBinaryStream(String columnLabel, InputStream x, long length) 
	{

	}


    public Reader getNCharacterStream(String columnLabel) 
	{
	    return getNCharacterStream(columnLabel);
	}

    public Reader getNCharacterStream(int columnIndex) 
	{
	    return getNCharacterStream(columnIndex);
	}


    public String getNString(int columnIndex)
	{
	    return getNString(columnIndex);
	}

    public String getNString(String columnLabel) 
	{
	    return getNString(columnLabel);
	}


    public void 	updateSQLXML(int columnIndex, SQLXML xmlObject)
	throws SQLException
	{
	    resultSet.updateSQLXML(columnIndex, xmlObject);
	}


    public void 	updateSQLXML(String columnLabel, SQLXML xmlObject)
	throws SQLException
	{
	    resultSet.updateSQLXML(columnLabel, xmlObject);
	}


    public SQLXML 	getSQLXML(int columnIndex)
	throws SQLException
	{
	    return resultSet.getSQLXML(columnIndex);
	}

    public SQLXML 	getSQLXML(String columnLabel) 
	throws SQLException
	{
	    return resultSet.getSQLXML(columnLabel);
	}
    
    public NClob 	getNClob(int columnIndex)
	throws SQLException
	{
	    return resultSet.getNClob(columnIndex);
	}

    public NClob 	getNClob(String columnLabel) 
	throws SQLException
	{
	    return resultSet.getNClob(columnLabel);
	}


    public void 	updateNString(String columnLabel, String nString) 
	{

	}

    public boolean isClosed()
	throws SQLException
	{
	    return resultSet.isClosed();
	}

    public int getHoldability() 
	throws SQLException
	{
	    return resultSet.getHoldability();
	}

    public void 	updateRowId(int columnIndex, RowId x)
	{

	}

    public void 	updateRowId(String columnLabel, RowId x) 
	{

	}

    public RowId getRowId(String columnLabel) 
	throws SQLException
	{
	    return resultSet.getRowId(columnLabel);
	}

    public RowId getRowId(int columnIndex)
	throws SQLException
	{
	    return resultSet.getRowId(columnIndex);
	}

    public void 	updateArray(int columnIndex, Array x)
	{

	}

    public void 	updateArray(String columnLabel, Array x) 
	{

	}


    public void 	updateClob(int columnIndex, Clob x)
	{

	}

    public void updateRef(String columnLabel, Ref x) 
	{

	}

    public void updateRef(int columnIndex, Ref x) 
	{

	}

    public URL 	getURL(int columnIndex)	
	throws SQLException
	{
	    return resultSet.getURL(columnIndex);
	}


    public URL 	getURL(String columnLabel) 
	throws SQLException
	{
	    return resultSet.getURL(columnLabel);
	}



    /*
    public InputStream 	getUnicodeStream(String columnLabel) 
	{

	}
    */

    @Deprecated
    public BigDecimal 	getBigDecimal(int columnIndex, int scale) 
	throws SQLException
	{
	    return getBigDecimal(columnIndex, scale);
	}

    @Deprecated
    public BigDecimal 	getBigDecimal(String columnLabel, int scale) 
	throws SQLException
	{
	    return resultSet.getBigDecimal(columnLabel, scale);
	}

    public boolean isWrapperFor(Class<?> iface)
	throws SQLException
	{
	    return false;
	}

    public <T> T unwrap(Class<T> iface)
         throws SQLException
	{
	    return null;
	}

    /**
     * Get the <code>Verbose</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isVerbose() {
	return verbose;
    }

    /**
     * Set the <code>Verbose</code> value.
     *
     * @param newVerbose The new Verbose value.
     */
    public final void setVerbose(final boolean newVerbose) {
	this.verbose = newVerbose;
    }


    /*
    protected void finalize() throws Throwable {

	try {
	    System.err.println("Closing resultset file");
	    if(oout2 != null)
		oout2.close();        // close open files
	} finally {
	    super.finalize();
	}
    }
    */

	public <T> T	getObject(int columnIndex, Class<T> type) throws SQLException, SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException();
	}
 
	public <T> T 	getObject(String columnLabel, Class<T> type) throws SQLException, SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException();
		
	}	

	

}
