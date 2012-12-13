package ca.gnewton.lusql.driver.tee;
import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class TeeDocSink 
	extends AbstractDocSink
{
	List<DocSink>sinks = new ArrayList<DocSink>();
	List<ReentrantLock>locks = new ArrayList<ReentrantLock>();
    
	public TeeDocSink() {

	}
    
	@Override
	public String description()
	{
		return "Sends a Document to multiple DocSinks";
	}

	public static String TeeDocSinkKey = "teeDocSink";
	public static String TeeDocSinkAttributesKey = "teeParameter";
    
	// java -jar lusql.jar  -psi teeDocSink=0:ca.gnewton.lusql.driver.lucene.LuceneDocSink -psi teeProperty=0:lusql_bufferSize=128 -psi teeProperty=0:lusql_sinkLocation=./index   -psi teeDocSink=1:ca.gnewton.lusql.driver.file.PrintDocSink 


	@Override
	public Properties explainProperties()
	{
		Properties p = new Properties();
		p.setProperty(TeeDocSinkKey, 
		              "Class name of DocSink, with the number N and a colon prepended to the value. For example, to use 2 Sinks, ca.gnewton.lusql.driver.lucene.LuceneDocSink and ca.gnewton.lusql.driver.file.PrintDocSink, you would use the following: "
		              + " -psi " 
		              + TeeDocSinkKey
		              + "=0:ca.gnewton.lusql.driver.lucene.LuceneDocSink"

		              + " -psi " 
		              + TeeDocSinkKey
		              + "=1:ca.gnewton.lusql.driver.file.PrintDocSink"
		              );

		p.setProperty(TeeDocSinkAttributesKey, "Parameter for underlying DocSink. This has the structure: \'"
		              + TeeDocSinkAttributesKey 
		              + "=N:prop=value\' where N is the number of the Sink, and para=value are the Sink-specific parameter and value"
		              );

		p.setProperty("foobar", "hello how are you?");
		return p;
	}


	@Override
	public void init(MultiValueProp p) throws PluginException
	{
		// Get the sinks
		List<String> sinkNames = p.get(TeeDocSinkKey);
		//List<MultiValueProp> sinkProps = new ArrayList<MultiValueProp>();
		Map<Integer, MultiValueProp> sinkProps = extractSinkProperties(p);
	    
		for(int i=0; i<sinkNames.size(); i++)
			{
				String sinkName = sinkNames.get(i);
				MultiValueProp mvp = new MultiValueProp();
				//DocSink docSink = LuSql.makeDocSink(sinkName);
				DocSink docSink = null;
				try
					{
						docSink = (DocSink)ca.gnewton.lusql.util.Util.newPlugin(sinkName);
					}
				catch(Throwable t)
					{
						t.printStackTrace();
						throw new PluginException("TeeDocSink.init(): Unable to create plugin: " 
						                          + sinkName);
					}
				docSink.init(sinkProps.get(i));
				sinks.add(docSink);
				//sinkProps.add(mvp);
			}
	}
    
	Map<Integer, MultiValueProp> extractSinkProperties(MultiValueProp p)
	{
		List<String> inProps = p.get(TeeDocSinkAttributesKey);
		Map<Integer, MultiValueProp> outProps = new HashMap<Integer, MultiValueProp>();
	
		for(String prop: inProps)
			{
				// teeProperty=0:lusql_bufferSize=128		    
				String parts[] = prop.split(":");
				int n = Integer.parseInt(parts[0]);
				parts = parts[1].split("=");
				Integer i = new Integer(n);
				MultiValueProp mvp = null;
				if(outProps.containsKey(i))
					mvp.get(i.toString());
				else
					{
						mvp = new MultiValueProp();
						outProps.put(i, mvp);
					}
		
				mvp.put(parts[0], parts[1]);
			}
		//extractProperties(p);
		return outProps;
	}
    



	@Override
	public void addDoc(Doc[] docs)  
		throws DocSinkException
	{
		for(int i=0; i<sinks.size(); i++)
			{
				ReentrantLock lock = locks.get(i);
				DocSink sink = sinks.get(i);
				lock.lock();
				try
					{
						AddDocument ad = new AddDocument();
						ad.setDocSink(sink);
						ad.setDocs(docs);
						ad.run();
					} 
				finally 
					{
						lock.unlock();
					}
			}
		
	}

	@Override
	public void setPrimaryKeyField(String f)
	{
	
	}
    
	@Override
	public void setRemoveOnDone(boolean b)
	{
	
	}

	@Override
	public void done() throws PluginException
	{
		boolean hasException = false;
		for(DocSink sink: sinks)
			{
				try
					{
						sink.done();
					}
				catch(Throwable t)
					{
						t.printStackTrace();
						hasException = true;
					}
			}
		if(hasException)
			throw new PluginException("TeeDocSink: One of the DocSinks failed in done()");
	}
    

	@Override
	public void commit() throws DocSinkException
	{
		boolean hasException = false;
		for(DocSink sink: sinks)
			{
				try
					{
						sink.commit();
					}
				catch(Throwable t)
					{
						t.printStackTrace();
						hasException = true;
					}
			}
		if(hasException)
			throw new DocSinkException("TeeDocSink: One of the DocSinks failed in commit()");
	}

	public Object internal()  throws DocSinkException
	{
		return null;
	}

	public String showState(int n) 
		throws PluginException    
	{
		StringBuilder sb = new StringBuilder();
		sb.append(ca.gnewton.lusql.util.Util.offset("TeeDocSink",n));
		sb.append(ca.gnewton.lusql.util.Util.offset(" " + sinks.size() + " DocSinks",n));
		for(DocSink sink: sinks)
			{
				sb.append(ca.gnewton.lusql.util.Util.offset("Sub DocSinks:",n));
				sb.append(sink.showState(n+1));
			}
		return sb.toString();
	}

} //
