package ca.gnewton.lusql.driver.bdb;
import java.io.*;
import java.util.*;

import org.apache.lucene.store.*;
import com.sleepycat.je.*;
import com.sleepycat.persist.*;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;


public class BDBDocSink
	extends AbstractDocSink
{
	@Override
	public String description()
	{
		return "Sink that writes documents to Berkeley DB";
	}

	public BDBDocSink()
	{
		setSupportsCompression(false);
	}
    
	@Override
	public boolean requiresPrimaryKeyField()
	{
		return true;
	}

	public static final String StoreName = BDBCore.StoreName;

	/**
	 * Describe create here.
	 */
	private boolean create;

	/**
	 * Describe indexDirName here.
	 */
	private String indexDirName;

	private boolean readOnly = false;

	BDBCore core = null;

	Environment myDbEnv = null;
	EnvironmentConfig envConfig = null;
	EntityStore store = null;
	BDBDocWrapperDA bda = null;

	@Override    
	public void init(MultiValueProp p) 
		throws PluginException
	{
		setThreadSafe(false);
		extractProperties(p);
		try
			{
				// Open the environment.
				try 
					{
						core = new BDBCore(getIndexDirName(), isCreate(), isReadOnly());
					} 
				catch (Throwable t) 
					{
						t.printStackTrace();
						throw new PluginException("Problem instantiating BDB environment/db");
					}
			}
		catch(Throwable t)
			{
				t.printStackTrace();
				throw new PluginException();
			}
	}


	public Properties explainProperties()
	{
		return null;
	}


	public void done() throws PluginException
	{
		core.done();
	}


	public void addDoc(Doc[] docList)  throws DocSinkException
	{
		for(Doc doc: docList)
			{
				if(doc.isLast()){
					break;
				}
			
				BDBDocWrapper w = new BDBDocWrapper();
				try
					{
						try
							{
								w.setId(doc.getFieldValues(getPrimaryKeyField()).get(0));
							}
						catch(Throwable t)
							{
								t.printStackTrace();
								System.err.println("\n>> Error in BDBDocSink. Did you set the primary key with \"-P\"?\n");
								System.out.println(doc);
				    
								throw new DocSinkException();
							}
						w.addDoc(doc);
						//bda.pIdx.put(w);
						core.put(w);
					}
				catch(Throwable t)
					{
						t.printStackTrace();
						throw new DocSinkException();
					}
			}
	}//

	public Object internal()  throws DocSinkException
	{
		return null;
	}

	public boolean isRemoveOnDone()
	{
		return false;
	}

	public void commit() throws DocSinkException
	{

	}


	////////////////

	/**
	 * Describe <code>main</code> method here.
	 *
	 * @param args a <code>String</code> value
	 */
	public static final void main(final String[] args) 
	{
		if(args.length <1)
			{
				System.out.println("Writing...");
				write();
			}
		else
			read();
	}

	static void read()
	{
		try
			{
				MultiValueProp p = new MultiValueProp();
				p.setProperty(LuSqlFields.CreateSinkKey, "false");
				p.setProperty(LuSqlFields.SinkLocationKey, "bdbIndex");
		
				BDBDocSource b = new BDBDocSource();
				b.init(p);

				/*
				  PrimaryIndex<String,BDBDocWrapper> pi = 
				  b.store.getPrimaryIndex(String.class, BDBDocWrapper.class); 
				  EntityCursor<BDBDocWrapper> pi_cursor = pi.entities();
				*/
				Doc doc = null;
				while(true)
					{
						doc = b.next();
						if(doc.isLast())
							break;
						System.out.println(doc);
					}
			} 
   
		catch(Throwable t)
			{
				t.printStackTrace();
			}
	}

	static void write()
	{
		try
			{
				MultiValueProp p = new MultiValueProp();
				p.setProperty(LuSqlFields.CreateSinkKey, "true");
				p.setProperty(LuSqlFields.SinkLocationKey, "bdbIndex");
		
				BDBDocSink b = new BDBDocSink();
				b.setPrimaryKeyField("foo");
		
				b.init(p);
				for(int i=0; i<10; i++)
					{
						Doc doc = new DocImp();
						doc.addField("foo", "fooValue"+i, null);
						doc.addField("foo", "fooValue2"+i, null);
						doc.addField("foo2", "foo2Value"+i, null);
						Doc[] dd = new Doc[1];
						dd[0] = doc;
						b.addDoc(dd);
						//b.addDoc(doc);
					}

				b.done();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}
	}

	void extractProperties(MultiValueProp p)
	{
		if(p.containsKey(LuSqlFields.CreateSinkKey))
			setCreate(Boolean.parseBoolean(p.getProperty(LuSqlFields.CreateSinkKey).get(0)));
		if(p.containsKey("readOnly"))
			setReadOnly(Boolean.parseBoolean(p.getProperty("readOnly").get(0)));

		if(p.containsKey(LuSqlFields.SinkLocationKey))
			setIndexDirName(p.getProperty(LuSqlFields.SinkLocationKey).get(0));
	}

	/**
	 * Get the <code>Create</code> value.
	 *
	 * @return a <code>boolean</code> value
	 */
	public final boolean isCreate() {
		return create;
	}

	/**
	 * Set the <code>Create</code> value.
	 *
	 * @param newCreate The new Create value.
	 */
	public final void setCreate(final boolean newCreate) {
		this.create = newCreate;
	}

	/**
	 * Get the <code>IndexDirName</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getIndexDirName() {
		return indexDirName;
	}

	/**
	 * Set the <code>IndexDirName</code> value.
	 *
	 * @param newIndexDirName The new IndexDirName value.
	 */
	public final void setIndexDirName(final String newIndexDirName) {
		this.indexDirName = newIndexDirName;
	}

	/**
	 * Get the <code>PrimaryKeyField</code> value.
	 *
	 * @return a <code>String</code> value
	 */
	public final String getPrimaryKeyField() {
		return primaryKeyField;
	}


	public final boolean isReadOnly() {
		return readOnly;
	}

	/**
	 * Set the <code>ReadOnly</code> value.
	 *
	 * @param newReadOnly The new ReadOnly value.
	 */
	public final void setReadOnly(final boolean newReadOnly) {
		this.readOnly = newReadOnly;
	}

	public String descriptor()
	{
		return this.getClass().getName() + ": Directory: " + getIndexDirName();
	}

	public String showState(int n) 
		throws PluginException    
	{
		StringBuilder sb = new StringBuilder();
		sb.append(ca.gnewton.lusql.util.Util.offset(super.showState(n), n));
	
		sb.append(ca.gnewton.lusql.util.Util.offset("read-only:" + readOnly,n+1));
		sb.append(ca.gnewton.lusql.util.Util.offset("primary key field:" + primaryKeyField,n+1));
		sb.append(ca.gnewton.lusql.util.Util.offset("index dir name:" + indexDirName,n+1));
		sb.append(ca.gnewton.lusql.util.Util.offset("create:" + create,n+1));
		//sb.append(ca.gnewton.lusql.util.Util.offset("log file max:" + BDBCore.logFileMax, n+1));
		//sb.append(ca.gnewton.lusql.util.Util.offset("cleaner lookahead cache size:" + BDBCore.cleanerLookAheadCacheSize, n+1));
		sb.append(ca.gnewton.lusql.util.Util.offset("storeName:" + BDBCore.StoreName, n+1));

		return sb.toString();
	}
    
}//
