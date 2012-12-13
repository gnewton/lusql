package ca.gnewton.lusql.core;
import org.apache.lucene.index.*;
import java.sql.Connection;

/**
 * Describe interface LuSqlFields here.
 *
 *
 * Created: Sun Sep 14 17:02:22 2008
 *
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public interface LuSqlFields 
{

    public static final String DefaultDocFilterClassName = "ca.gnewton.lusql.core.NullFilter";

    public static final String DefaultDocSinkClassName = "ca.gnewton.lusql.driver.lucene.LuceneDocSink";
    public static final String DefaultDocSourceClassName = "ca.gnewton.lusql.driver.jdbc.JDBCDocSource";

    public static final String DefaultJDBCDriverClassName = "com.mysql.jdbc.Driver";

	public static final String DefaultAnalyzerClassName = ca.gnewton.lusql.driver.lucene.LuceneCore.DEFAULT_ANALYZER;

    public static final String DefaultSinkLocationName = "index";

    public static final String LuSqlInfoSuffix = ".lusql.txt";

    final public static int DefaultDocPacketSize = 16;    
    static public int DefaultChunkSize = 10000;
    static public int DefaultWorkPerThread = 10;
    static public int MinChunkSize = 333;
    static public int OffsetDefault = 0;

    static public int DefaultTransactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED;

    //static public double DefaultRAMBufferSizeMB = IndexWriter.DEFAULT_RAM_BUFFER_SIZE_MB;
    static public double DefaultRAMBufferSizeMB = 48;



    // Properties sent to plugins
    //   Generic
    static public String SinkLocationKey = "lusql_sinkLocation";
    static public String SecondarySinkLocationKey = "lusql_secondardSinkLocation";
    static public String SourceLocationKey = "lusql_sinkLocation";
    static public String MergeSinksOnDoneKey = "lusql_mergeSinksOnDone";
    static public String RemoveSinksOnDoneKey = "lusql_removeSinksOnDone";
    static public String AppendToSinkKey = "lusql_appendToSink";
    static public String QueryKey = "lusql_Query";
    static public String CreateSinkKey = "lusql_createSink";
    static public String StopWordFileNameKey = "lusql_stopWordFile";
    //   Lucene oriented
    static public String AnalyzerClassKey = "lusql_LuceneAnalyzer";
    static public String BufferSizeKey = "lusql_bufferSize";
    //   JDBC oriented
    public static final String IsMysqlKey = "lusql_isMysql";
    public static final String JDBCDriverClassKey = "lusql_jdbcDriverClass";
    public static final String DBUrlKey = "lusql_dbUrl";
    public static final String JDBCUserKey = "lusql_jdbcUser";
    public static final String JDBCDriverKey = "lusql_jdbcDriver";
    public static final String JDBCPasswordKey = "lusql_jdbcPassword";
    static public final String JDBCFetchSizeKey = "lusql_jdbcFetchSize";

    // Names for CLI options
    public static final String CLIDocSinkClassName="si";
    public static final String CLIDocSourceClassName="so";
    public static final String CLIDocFiltersClassName="f";

    public static final String CLIDocFilterProperties="pf";
    public static final String CLIDocSourceProperties="pso";
    public static final String CLIDocSinkProperties="psi";
    
}
