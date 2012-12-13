package ca.gnewton.lusql.core;
 
import java.io.*;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.*;
import org.apache.commons.cli.*;
import org.apache.log4j.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class LuSqlMain here.
 *
 *
 * Created: Mon Nov 26 16:30:11 2007
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 */

public class LuSqlMain
    implements LuceneFields, LuSqlFields, LocalAnnotation
{
	//static Category cat = Category.getInstance(LuSql.class.getName());
    static
    {
	    BasicConfigurator.configure();
    }

    /**
     * Describe explainPlugins here.
     */
    static private String[] explainPlugins = null;

    public enum RunState 
	{
	    ShowOptions,
			ExplainPlugin,
			Done,
			Work
			};

    boolean compressFromSource = false;
    boolean compressToSink = false;

    static Map<String, String>fieldMap = new HashMap<String,String>();

    static Options options = new Options();
    /**
     * Creates a new <code>LuSql</code> instance.
     *
     */
    public LuSqlMain() 
	{

	}

    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) 
	{
	    long t0 = System.currentTimeMillis();
	    LuSql lusql = new LuSql();
	    lusql.setFieldMap(fieldMap);
	    boolean optionsFlag;
	    try
			{
				RunState state = handleOptions(lusql, args);
				switch(state)
					{
					case Done:
						break;
					case Work:
						// Txt file describing how index was made//
						initInfoFile(lusql);
						Util.setOut(infoOut);

						lusql.init();
						if(lusql.isVerbose())
							printOptions(lusql);			
						lusql.run();
						if(lusql.isVerbose())
							System.err.println("*********** Elapsed time: " + (System.currentTimeMillis() - t0)/1000 + " seconds\n");
						if(infoOut != null)
							infoOut.close();
						break;
					case ExplainPlugin:
						explainPlugin();
						break;

					case ShowOptions:
					default:
						usage();
						break;
					}
			}
	    catch(Throwable pe)
			{
				pe.printStackTrace();
				usage();
				return;
			}
	}


    static void describePlugin()
	{
	    
	}

    static void printOptions(LuSql lusql)
	{
	    Util.msg("Using Source:\t" + lusql.getDocSourceClassName(), false);
	    Util.msg("Using Sink:\t" + lusql.getDocSinkClassName(), false);
	    Util.msg("Using sql:" + Util.delim(lusql.getQuery()),false);
	    if(lusql.getAnalyzerName() == null)
			{
				lusql.setAnalyzerName(DefaultAnalyzerClassName);
				Util.msg("Using default analyzer", false);		
			}
	    Util.msg("Using Analyzer:" + Util.delim(lusql.getAnalyzerName()),false);
	    Util.msg("Using Stop Word FileName:" + Util.delim(lusql.getStopWordFileName()),false);
	    Util.msg("Using Properties FileName:" + Util.delim(lusql.getPropertiesFileName()),false);
	    Util.msg("Using Global field parameters: " + lusql.getGlobalFieldIndexParameter(), false);
	    Map<String, LuceneFieldParameters>  paras = lusql.getFieldIndexParameters();
	    if(paras != null && lusql.isVerbose())
			{
				Iterator<String> it = paras.keySet().iterator();
				while(it.hasNext())
					{
						String f = it.next();
						System.err.println("Using Lucene paramaters for field: " + f
										   + "  " + paras.get(f));
					}
			}


	    if(lusql.getDBDriverName() == null)
			{
				lusql.setDBDriverName(DefaultJDBCDriverClassName);
				Util.msg("Using default JDBC Driver", false);		
			}
	    Util.msg("Using DB driver name:" + Util.delim(lusql.getDBDriverName()),false);
	    Util.msg("Using DB URL:" + Util.delim(lusql.getDBUrl()),false);
	    Util.msg("Using DocSink destination (i.e. Lucene index):" + lusql.getSinkLocationName(), false);
	    Util.msg("Using Lucene index RAMBUFFER MBs:" + lusql.getRAMBufferSizeMB(), false);

	    Util.msg("Using multithreaded:" + lusql.isThreaded(), false);
	    if(lusql.isThreaded())
			{
				Util.msg("\tUsing # threads:" + lusql.getNumThreads(), false);
				Util.msg("\tUsing queue size:" + lusql.makeQueueSize(), false);
			}
	    Util.msg("Using Test:" + lusql.isTest(), false);
	    /*
		  if(lusql.getFieldIndexParameters() == null)
		  Util.msg("Using Field parameters:" + formatFieldParameters(lusql), false);

		  else
		  Util.msg("Using Field Index Parameters:" + Util.delim(lusql.getFieldIndexParameters()),false);		
	    */
	    if(lusql.isMySql())
			Util.msg("Using setting DB fetchsize=0 (see -m)", false);
	    else
			Util.msg("UsingDB is NOT MySql", false);
	    Util.msg("Using Num documents to add:" + lusql.getMaxDocs(), false);

	    Util.msg("Using -Q SQL replacement character:" + SubQuery.getKeyMeta(), false);
	    Iterator<SubQuery>sqs = lusql.getSubQueries().iterator();
	    while(sqs.hasNext())
			Util.msg("Using -Q SQL:" + sqs.next().getQuery(), false);

	    

	    // Filters
	    List<String> filters = lusql.getDocFilterNames();
	    Util.msg("Using filters:", false);
	    if(filters == null || filters.size() == 0)
			Util.msg("\tnone", false);
	    else
			{
				for(String filterName: filters)
					Util.msg("\t" + filterName, false);
				// Filter properties
				Map<String, MultiValueProp> fp = lusql.getFilterProperties();
				Iterator<String> it = fp.keySet().iterator();
				while(it.hasNext())
					{
						String k = it.next();
						System.err.println("\tFilter properties: " + k + "=" + fp.get(k));
					}
			}

	    //

	    System.err.println("**************************************");

	    try
			{
				System.err.print(lusql.getDocSource().showState(0));
				System.err.print(lusql.getDocSink().showState(0));
			}
	    
	    catch(Throwable t)
			{
				t.printStackTrace();
				return;
			}
	    

	    // Field Mappings
	    Map<String, String>fieldMap = lusql.getFieldMap();
	    Iterator<String>it = fieldMap.keySet().iterator();
	    if(fieldMap.size()>0)
			{
				Util.msg("Using field mappings:", false);
				while(it.hasNext())
					{
						String key = it.next();
						Util.msg("\tSource." + key + " => Sink." + fieldMap.get(key), false);
					}
			}

	    // Fields to include
	    Set<String>fieldNames = lusql.getFieldNames();
	    if(fieldNames != null && fieldNames.size() > 0)
			{
				System.err.print("Using only fields: ");
				for(String fn: fieldNames)
					System.err.print(fn + "; ");
				System.err.println("");
			}
	    
	}



    static public RunState handleOptions(LuSql lusql, final String[] args)
		throws ParseException
	{
	    setupOptions();

	    if(args.length == 0)
			{
				return RunState.ShowOptions;
			}

	    RunState optionFlag = RunState.Work;
	    //CommandLineParser parser = new PosixParser();
	    CommandLineParser parser = new GnuParser();
	    CommandLine line = null;
	    
	    try
			{
				line = parser.parse( options, args );
			}
	    catch(Exception e)
			{
				e.printStackTrace();
				return RunState.ShowOptions;
			}

	    //***********************************************
	    // This must be set before other things...
	    //***********************************************
	    if(line.hasOption("I"))
			{
				lusql.setGlobalFieldIndexParameter(new LuceneFieldParameters(line.getOptionValue("I")));
			}
	    //***********************************************


	    String[] explain = line.getOptionValues("e");
	    if(explain != null)
			{
				try
					{
						setExplainPlugins(explain);
					}
				catch(Throwable t)
					{
						t.printStackTrace();
					}
				return RunState.ExplainPlugin;
			}

	    String[] sinkProps = line.getOptionValues(CLIDocSinkProperties);
	    MultiValueProp sinkP = new MultiValueProp();
	    if(sinkProps != null)
			{

				for(int i=0; i<sinkProps.length; i++)
					{
						String parts[] = sinkProps[i].split("=");
						if(parts == null || parts.length != 2)
							{
								System.err.println("Error processing command line parameter:[-psi"
												   + sinkProps[i]
												   + "]  Missing equals sign?"
												   );
							}
						sinkP.put(parts[0], parts[1]);
					}

			}
	    lusql.setSinkProperties(sinkP);


	    MultiValueProp sourceP = new MultiValueProp();
	    String[] sourceProps = line.getOptionValues(CLIDocSourceProperties);
	    if(sourceProps != null)
			{
				for(int i=0; i<sourceProps.length; i++)
					{
						String parts[] = sourceProps[i].split("=");
						if(parts == null || parts.length != 2)
							{
								System.err.println("Error processing command line parameter:[ -pso"
												   + sourceProps[i] 
												   + "]  Missing equals sign?"
												   );
							}
						sourceP.put(parts[0], parts[1]);
					}
			}
	    lusql.setSourceProperties(sourceP);


	    /////////////////
	    String[] filterProps = line.getOptionValues(CLIDocFilterProperties);
	    if(filterProps != null)
			{
				Map<String, MultiValueProp> fp = new HashMap<String, MultiValueProp>();
				for(int i=0; i<filterProps.length; i++)
					{
						String parts[] = filterProps[i].split(":");
						if(parts == null || parts.length != 2)
							throw new ParseException("Badly formed filter options: " + filterProps[i]);
						String n = parts[0];
						parts = parts[1].split("=");
						MultiValueProp p = null;
						if(fp.containsKey(n))
							p = fp.get(n);
						else
							{
								p = new MultiValueProp();
								fp.put(n, p);
							}
						p.setProperty(parts[0], parts[1]);
					}

				Iterator<String> it = fp.keySet().iterator();
				while(it.hasNext())
					{
						String k = it.next();
						lusql.setFilterProperties(k, fp.get(k));
					}
			}
		    
	    if(line.hasOption("O"))
			{
				lusql.setOffset(Integer.parseInt(line.getOptionValue("O")));
			}


	    if(line.hasOption("stdout"))
			{
				lusql.setSinkWriteToStdout(true);
			}

	    if(line.hasOption("stdin"))
			{
				lusql.setSinkReadFromStdin(true);
			}



	    // Global fields
	    String[] gfs = line.getOptionValues("g");
	    if(gfs != null)
			for(int i=0; i<gfs.length; i++)
				{
					String nnnParts[] = gfs[i].split("\\|");

					String nnn = null;
					String theRest = gfs[i];
					LuceneFieldParameters lfp = lusql.getGlobalFieldIndexParameter();			

					if(nnnParts.length == 2)
						{
							// -g "A:A:A|field=value"
							lfp = new LuceneFieldParameters(nnnParts[0]);
							theRest = nnnParts[1];
						}
					// else -g "field=value"

					String parts[] = theRest.split("=");
					if(parts == null || parts.length != 2)
						{
							System.err.println("Bad global field parameter (-g): " +gfs);
							return RunState.ShowOptions;
						}
					lusql.addGlobalField(lfp, parts[0], parts[1]);
				}

	    String[] sqs = line.getOptionValues("Q");
	    String parameter;
	    if(sqs != null)
			for(int i=0; i<sqs.length; i++)
				{
					//System.err.println("****** " + sqs[i]);
					String[] parts = sqs[i].split("|");

					//if(parts.length == 3)
						parameter = parts[1];
					//else
					//parameter = DefaultFieldIndexGlobalParameter;
					lusql.addSubQuery(new SubQuery(sqs[i],
												   parameter));
				}


	    if(line.hasOption("M"))
			SubQuery.setKeyMeta(line.getOptionValue("M"));

	    if(line.hasOption("zso"))
			lusql.setSourceCompression(true);
	    if(line.hasOption("zsi"))
			lusql.setSinkCompression(true);

	    if(line.hasOption("A"))
			{
				lusql.setAppendToLuceneIndex(true);
				lusql.setIndexCreate(false);
			}
	    
	    if(line.hasOption("a"))
			lusql.setAnalyzerName(line.getOptionValue("a"));
	    
	    if(line.hasOption("C"))
			lusql.setOutputChunk((Integer.parseInt(line.getOptionValue("C"))));

	    if(line.hasOption("w"))
			lusql.setWorkPerThread((Integer.parseInt(line.getOptionValue("w"))));


	    if(line.hasOption("S"))
			lusql.setQueueSize(Integer.parseInt(line.getOptionValue("S")));

	    if(line.hasOption("V"))
			lusql.setLoadAverageLimit((Float.parseFloat(line.getOptionValue("V"))));

	    //REQUIRED
	    if(line.hasOption("c"))
			{
				lusql.setDBUrl(line.getOptionValue("c"));
			}
	    /*
		  else
		  {
		  if(!line.hasOption("e"))
		  return RunState.ShowOptions;
		  }
	    */
			
	    if(line.hasOption("d"))
			{
				lusql.setDBDriverName(line.getOptionValue("d"));
				System.err.println("****************************");
			}


	    String[] flds = line.getOptionValues(CLIDocFiltersClassName);
	    if(flds!=null)
			{
				List<String> filterNames = new ArrayList<String>();
				for(int i=0; i<flds.length; i++)
					{
						filterNames.add(flds[i]);
					}
				lusql.setDocFilterNames(filterNames);
			}


	    flds = line.getOptionValues("F");
	    if(flds!=null)
			{
				Set<String> fieldNames = new HashSet<String>();
				for(int i=0; i<flds.length; i++)
					{
						fieldNames.add(flds[i]);
					}
				lusql.setFieldNames(fieldNames);
			}


	    // -i fieldname=A:A:A
	    flds = line.getOptionValues("i");
	    if(flds!=null)
			{

				for(int i=0; i<flds.length; i++)
					{
						String[] parts = flds[i].split("=");
						if(parts == null || parts.length != 2)
							{
								System.err.println("Bad global field parameter (-i): " +flds[i]);
								System.err.println("");
								return RunState.ShowOptions;
							}
			
						System.out.println("   -i=" + parts[0]
										   + "==" + parts[1]);
			
						lusql.addFieldIndexParameter(parts[0], parts[1]);
					}
			}

	    if(line.hasOption("l"))
			lusql.setSinkLocationName(line.getOptionValue("l"));

	    if(line.hasOption(CLIDocSourceClassName))
			lusql.setDocSourceClassName(line.getOptionValue(CLIDocSourceClassName));

	    if(line.hasOption("P"))
			lusql.setPrimaryKeyField(line.getOptionValue("P"));

	    if(line.hasOption(CLIDocSinkClassName))
			{
				lusql.setDocSinkClassName(line.getOptionValue(CLIDocSinkClassName));
			}
	    
		    


	    String[] sind = line.getOptionValues("L");
	    if(sind != null)
			for(int i=0; i<sind.length; i++)
				{
					lusql.addSecondaryIndexName(sind[i]);
					lusql.setConcurrentDocSink(true);
				}

	    if(line.hasOption("J"))
			lusql.setMerge(false);

	    if(line.hasOption("m"))
			lusql.setMySql(false);

	    if(line.hasOption("N"))
			lusql.setNumThreads(new Integer(line.getOptionValue("N")).intValue());

	    if(line.hasOption("K"))
			lusql.setDocPacketSize(new Integer(line.getOptionValue("K")).intValue());

	    if(line.hasOption("n"))
			lusql.setMaxDocs(new Long(line.getOptionValue("n")).longValue());

	    if(line.hasOption("p"))
			{
				lusql.setPropertiesFileName(line.getOptionValue("p"));
				try
					{
			
						MultiValueProp mvp = new MultiValueProp();
						Properties p = new Properties();
						p.load(new FileInputStream(lusql.getPropertiesFileName()));
						mvp.loadFromProperties(p);
						lusql.setProperties(mvp);
					}
				catch(IOException io)
					{
						io.printStackTrace();
						throw new ParseException("Problem with properties file: " + lusql.getPropertiesFileName());
					}
			}

	    if(line.hasOption("q"))
			lusql.setQuery(line.getOptionValue("q"));

	    if(line.hasOption("r"))
			lusql.setRAMBufferSizeMB(new Double(line.getOptionValue("r")).doubleValue());

	    if(line.hasOption("s"))
			lusql.setStopWordFileName(line.getOptionValue("s"));

	    if(line.hasOption("t"))
			lusql.setTest(true);

	    if(line.hasOption("T"))
			lusql.setThreaded(false);

	    if(line.hasOption("E"))
			lusql.setTransactionIsolation(Integer.parseInt(line.getOptionValue("E")));


	    if(line.hasOption("v"))
			lusql.setVerbose(true);

	    if(line.hasOption("onlyMap"))
			lusql.setOnlyMap(true);

	    flds = line.getOptionValues("map");
	    if(flds!=null)
			{
				for(int i=0; i<flds.length; i++)
					{
						String[] parts = flds[i].split("=");
						fieldMap.put(parts[0], parts[1]);
					}
			}
	    	
	    return optionFlag;
	}


    static void setupOptions()
	{

	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Subquery in the form \"field|A:A:A|sql\" or \"field|A:A:A A:A:A...|sql\" or \"field|sql\"  (See -i for A:A:A values). Note that you can have multiple -Qs. Also note that putting a '*' before the field indicates you want the results cached (useful only if there is a possible for subsequent cache hits. Use only if you know what you are doing." )
						  .create("Q"));
		
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("For DocSinks (Indexes) that support multiple real indexes, either to eventual merging or as-is")
						  .create("L"));


	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Set static Document field and value. This is a field that has the same value for all saved documents. Format: \"field=value\" or \"A:A:A:field=value\" (See -i for A:A:A values)")
						  .create("g"));

	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Full name class implementing Lucene Analyzer; Default: " 
										   + LuSql.DefaultAnalyzerClassName)
						  .create("a"));

	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Offset # documents to ignore before indexing. Default:"
										   + LuSqlFields.OffsetDefault)
						  .create("O"));

	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Full name class implementing DocSink (the index class). Default: "
										   + LuSql.DefaultDocSinkClassName)
						  .create(CLIDocSinkClassName));

	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Full name class implementing DocSource (the index class). Default: "
										   + LuSql.DefaultDocSourceClassName)
						  .create("so"));

	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Primary key field name fron DocSource to be used in DocSink. Only for DocSinks that need it. Lucene does not. BDB does. For JDBCDocSource, if not set, uses first field in SQL query.")
						  .create("P"));


	    options.addOption("A", false, "Append to existing Lucene index.");


	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Queue size for multithreading. Default: numThreads * 50")
						  .create("S"));

	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("On Linux machines, tries to limit activity to keep load average below this value. Default: infinite")
						  .create("V"));


	    options.addOption("J", false, "For multiple indexes (see -L) do not merge. Default: false");

	    options.addOption("o", false, "If supported, have DocSink write to stdout");

			      
	    //////////////////////////
	    options.addOption(OptionBuilder.hasArg()
						  //.isRequired()
						  .withDescription("JDBC connection URL: REQUIRED")
						  .create("c"));

	    //////////////////////////
	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Verbose output chunk size. Default:" + LuSqlFields.DefaultChunkSize)
						  .create("C"));


	    //////////////////////////
	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Amount of documents to be processed per thread. Default:" 
										   + LuSqlFields.DefaultWorkPerThread
										   + ". Increasing tends to improve throughput; Decreasing tends to reduce memory problems and can alleviate an \"Out of memory\" exception. Should be 5-100 for medium/small documents. Should be 1 for very large documents.")
						  .create("w"));

	    //////////////////////////
	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Full name of DB driver class (should be in CLASSPATH); Default: " + LuSql.DefaultJDBCDriverClassName)
						  .create("d"));

	    
	    //////////////////////////
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Full name class implementing DocumentFilter; Default: " 
										   + LuSql.DefaultDocFilterClassName 
										   + " (does nothing). This is applied before each Lucene Document is added to the Index. If it returns null, nothing is added. Note that multiple filters are allowed. They are applied in the same order as they appear in the command line.")
						  .create(CLIDocFiltersClassName)
						  );



	    //////////////////////////
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Only include these fields from DocSource. Example: -F author -F id. Is absolute (i.e. even if you have additional fields - like in your SQL query - they will be filtered out.")
						  .create("F")
						  );


	    //////////////////////////
	    options.addOption("I", true, "Global field index parameters. This sets all the fields parameters to this one set. Format: A:A:A. See -i for A:A:A values. Note that -i has precedence over -I.");

	    //////////////////////////
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Size of internal arrays of documents. One of these arrays is put on the queue. So the number of objects waiting to be processed is K*S (array size * queue size). For small objects have more (k=100). For large objects have fewer (k=5). Default: " 
										   + LuSqlFields.DefaultDocPacketSize)
						  .create("K"));

	    //////////////////////////
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Full name plugin class; Get description and properties options needed by specific plugin (filter, source, or sink.")
						  .create("e"));

	    StringBuilder sb = new StringBuilder();
	    sb.append("One set per field in SQL, and in same order as in SQL. ");
	    sb.append("Used only if you want to overide the defaults (below). ");
	    sb.append("See for more information Field.Index, Field.Store, Field.TermVector in");
	    sb.append("org.apache.lucene.document.Field http://lucene.apache.org/java/3_0_2/api/core/org/apache/lucene/document/Field.html");
	    //http://lucene.apache.org/java/2_2_0/api/org/apache/lucene/document/Field.html");
	    sb.append("\nDefault: A:A:A= " 
				  //+ Util.getIndex(LuSql.IndexDefault, IndexParameterValues) 
				  //+ Util.getIndex(LuSql.StoreDefault, StoreParameterValues) 
				  //+ Util.getIndex(LuSql.TermVectorDefault, TermVectorParameterValues) 
				  + LuceneFieldParameters.rindex.get(LuSql.defaultLuceneFieldParameters.getIndex())
				  + ":"
				  + LuceneFieldParameters.rstorex.get(LuSql.defaultLuceneFieldParameters.getStore())
				  + ":"
				  + LuceneFieldParameters.rtermx.get(LuSql.defaultLuceneFieldParameters.getTermVector())
				  );
		      
	    sb.append("\nField Index Parameter values:");
	    sb.append("\nIndex: Default: " + LuceneFieldParameters.rindex.get(LuSql.defaultLuceneFieldParameters.getIndex()));
	    sb.append("\n");

	    Set<String>names= LuceneFieldParameters.indexx.keySet();
	    for(String name:names)
			{
				sb.append("\n-  " + name);
			}
	    

	    sb.append("\nStore: Default: " 
				  + LuceneFieldParameters.rstorex.get(LuSql.defaultLuceneFieldParameters.getStore()));

	    sb.append("\n");
	    names= LuceneFieldParameters.storex.keySet();
	    for(String name:names)
			{
				sb.append("\n-  " + name);
			}
	    sb.append("\n Term vector: Default: " 
				  + LuceneFieldParameters.rtermx.get(LuSql.defaultLuceneFieldParameters.getTermVector()));

	    sb.append("\n");
	    names= LuceneFieldParameters.termx.keySet();
	    for(String name:names)
			{
				sb.append("\n-  " + name);
			}
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Field index parameters. \nFormat: \"fieldName=A:A:A\". Note that -i can have a slightly different interpretation depending on the DocSource. For DocSource implementation where the syntax of the query allows for separate definition of the query and the fields of interest (like SQL), all of the fields defined in the query are stored/indexed. For other DocSource's where only the query can be defined and the fields of interest cannot (like the Lucene syntax of the LucenDocSource), the \"-i\" syntax is the only way to set the fields to be used. "
										   + sb)
						  .create("i")); 

	    //////////////////////
	    options.addOption(OptionBuilder.hasArg()
						  //.isRequired()
						  .withDescription("Sink destination (i.e. Lucene index to create/write to). Default: " + LuSql.DefaultSinkLocationName)
						  .create("l"));


	    //////////////////////
	    options.addOption("N", true, 
						  "Number of thread for multithreading. Defaults: Runtime.getRuntime().availableProcessors()) *2.5. For this machine this is: " 
						  + (Runtime.getRuntime().availableProcessors() *2.5)
						  );

	    //////////////////////
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Properties to be passed to the DocSource driver. Can be is multiple. Example: -pso foo=bar  -pso \"start=test 4\"")
						  .create("pso")); 


	    //////////////////////
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Properties to be passed to the DocSink driver. Can be multiple. See 'pso' for examples")
						  .create("psi")); 




	    //////////////////////
	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Properties to be passed to a filter. Can be multiple. Identify filter using integer (zero is the first filter). Example: -pf 0:size=10 -pf 0:name=fred -pf 1:reduce=true")
						  .create("pf")); 


	    //////////////////////
	    options.addOption(OptionBuilder
						  .withDescription("Read from source using source driver's internal compression, if it supports it")
						  .create("zso"));
  

	    options.addOption(OptionBuilder
						  .withDescription("Have sink driver use internal compression (opaque), if it supports it")
						  .create("zsi"));




	    //////////////////////
	    options.addOption("n", true, 
						  "Number of documents to add. If unset all records from query are used.");


	    //////////////////////
	    options.addOption("M", true,
						  "Changes the meta replacement string for the -Q command line parameters. Default: " + SubQuery.getKeyMeta());

	    //////////////////////
	    options.addOption("m", false, 
						  "Turns off need get around MySql driver-caused OutOfMemory problem in large queries. Sets Statement.setFetchSize(Integer.MIN_VALUE)" 
						  + "\n See http://benjchristensen.wordpress.com/2008/05/27/mysql-jdbc-memory-usage-on-large-resultset"
						  );

	    //////////////////////
	    options.addOption(OptionBuilder.hasArg()
						  .withDescription("Set JDBC Transaction level. Default: " 
										   + DefaultTransactionIsolation
										   + ". Values:\n" 
										   + Connection.TRANSACTION_NONE 
										   + " TRANSACTION_NONE\n"
										   + Connection.TRANSACTION_READ_UNCOMMITTED
										   + " TRANSACTION_READ_UNCOMMITTED\n"
										   + Connection.TRANSACTION_READ_COMMITTED
										   + " TRANSACTION_READ_COMMITTED\n"
										   + Connection.TRANSACTION_REPEATABLE_READ
										   + " TRANSACTION_REPEATABLE_READ\n"
										   + Connection.TRANSACTION_SERIALIZABLE
										   + " TRANSACTION_SERIALIZABLE\n "
										   + "(See http://java.sun.com/j2se/1.5.0/docs/api/constant-values.html#java.sql.Connection.TRANSACTION_NONE)")
						  .create("E"));

	    //////////////////////
	    options.addOption("p", true, "Properties file");

	    //////////////////////
	    //////////////////////
	    options.addOption(OptionBuilder.hasArg()
						  //.isRequired()
						  .withDescription("Primary SQL query (in double quotes). Only used by JDBC driver")
						  .create("q"));

	    //////////////////////
	    options.addOption("r", true, 
						  "LuceneRAMBufferSizeInMBs: IndexWriter.setRAMBufferSizeMB(). Only used by Lucene sinks. Default: "
						  + Double.toString(LuSql.DefaultRAMBufferSizeMB));

	    //////////////////////
	    options.addOption("s", true, 
						  "Name of stop word file to use (relative or full path). If supported by DocSource");

	    //////////////////////
	    options.addOption("T", false, 
						  "Turn off multithreading. Note that multithreading does not guarantee the ordering of documents. If you want the order of Lucene documents to match the ordering of DB records generated by the SQL query, turn-off multithreading"
						  );
	    
	    //////////////////////
	    options.addOption("t", false, 
						  "Test mode. Does not open up Lucene index. Prints (-n) records from SQL query");

	    //////////////////////
	    options.addOption("v", false, 
						  "Verbose mode");

	    //////////////////////
	    options.addOption("onlyMap", false, 
						  "Only use the fields from the DocSource that are mapped using -map");



	    options.addOption(OptionBuilder.hasArgs()
						  .withDescription("Field map. Transforms field names in DocSource to new fieldnames: Example -map \"AU=author\", where \"AU\" is the original (source) field name and \"author\" is the new (sink) field")
						  .create("map"));
	}//

    static List<String> makeRecordFields(String s)
	{
	    List<String> recordFields = new ArrayList<String>(1);
	    String[] result = s.split("\\s");
	    for (int x=0; x<result.length; x++)
			recordFields.add(result[x]);
	    return recordFields;
	}


    static public void usage()
	{
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp(120, "lusql", "", options, "--", true); 
	    //formatter.printHelp( "lusql", options );
	}

    static void explainPlugin()
		throws ClassNotFoundException,
			   NoSuchMethodException,
			   InstantiationException,
			   IllegalAccessException,
			   java.lang.reflect.InvocationTargetException
	{
	    String[] explain = getExplainPlugins();
	    for(int i=0; i<explain.length; i++)
			{
				Class<?> docSourceClass = Class.forName(explain[i]);
				Constructor constructor = docSourceClass.getConstructor();
				Plugin plugin = (Plugin)constructor.newInstance();
				String pluginType = "Plugin";
				if(plugin instanceof DocFilter)
					pluginType = "Filter";
				else
					if(plugin instanceof DocSource)
						pluginType = "Source";
				if(plugin instanceof DocSink)
					pluginType = "Sink";
				Properties ex = plugin.explainProperties();
				if(i > 0)
					System.err.println("\n------------------------------------------------------------");
				System.err.println(explain[i]);
				System.err.println("Description: " + plugin.description());


				System.err.println("Properties:");
		
				if(ex == null)
					{
						System.err.println("\tNo properties" );
						return;
					}
				Iterator<Object> it = ex.keySet().iterator();
				while(it.hasNext())
					{
						String key = (String)it.next();
						System.err.println("  " +  key + ": " + ex.get(key));
					}
		
			}
	}

    static BufferedWriter infoOut = null;

    static void initInfoFile(LuSql lusql)
	{
	    String initFile = lusql.getSinkLocationName() + LuSqlInfoSuffix;
	    try
			{
				infoOut = new BufferedWriter(new FileWriter(initFile));
			}
	    catch(Throwable t)
			{
				t.printStackTrace();
				throw new NullPointerException();
			}
	}

    /**
     * Get the <code>ExplainPlugins</code> value.
     *
     * @return a <code>String</code> value
     */
    static public final String[] getExplainPlugins() {
		return explainPlugins;
    }

    /**
     * Set the <code>ExplainPlugins</code> value.
     *
     * @param newExplainPlugins The new ExplainPlugins value.
     */
    static public final void setExplainPlugins(final String[] newExplainPlugins) {
		explainPlugins = newExplainPlugins;
    }
}
