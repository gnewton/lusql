package ca.gnewton.lusql.driver.sourcecheck;

import ca.gnewton.lusql.core.AbstractDocSink;
import ca.gnewton.lusql.core.AbstractDocSource;
import ca.gnewton.lusql.core.Doc;
import ca.gnewton.lusql.core.DocImpLast;
import ca.gnewton.lusql.core.DocSource;
import ca.gnewton.lusql.core.LuSql;
import ca.gnewton.lusql.core.Gettable; 
import ca.gnewton.lusql.core.PluginException;
import ca.gnewton.lusql.core.DocSinkException;
import ca.gnewton.lusql.util.MultiValueProp;
import java.util.concurrent.atomic.AtomicLong;

import java.util.Properties;
import java.util.Iterator;

public class DocSourceCheckSink
	extends AbstractDocSink
{
	public static final String DocSourceKey = "so";
	public static final String NumDocsKey = "n";

	private volatile AtomicLong count = new AtomicLong(0);
	private volatile boolean encounteredLast = false;

	private long numDocs = Long.MAX_VALUE;
	int docChunksCount = 0;

	private String sourceClassName = null;
	private MultiValueProp sourceProperties = null;
	
	
	@Override
	public Properties explainProperties()
	{
		Properties p = new Properties();

		p.setProperty(DocSourceKey, "The fully qualified class path name of the source to be used");
		p.setProperty("--null--", "All sink properties are sent down to the underlying source");
		
		return p;
	}

	public void init(MultiValueProp p) throws PluginException
	{
		extractProperties(p);
		DocSource source = LuSql.instantiateDocSource(sourceClassName);
		if(! (source instanceof Gettable)){
			throw new PluginException("DocSource ["
			                          + sourceClassName
			                          + "] does not implement ca.gnewton.lusql.core.Gettable");
			
		}
	}

	

	public void addDoc(Doc[] docs)  throws DocSinkException
	{
		++docChunksCount;
		for(Doc doc:docs){
			if(!doc.isLast()){
				count.incrementAndGet();
			}else{
				encounteredLast = true;
				numDocs = ((DocImpLast)doc).totalCount;
			}
			if(count.get() > numDocs){
				throw new DocSinkException("Count is greater than expected num docs: " 
				                           + count + " > " + numDocs);
			}

		}
	}

	static final private void checkCount(long c, long n)
		throws DocSinkException
	{
		if(c != n){
			throw new DocSinkException("Count is does not equal the expected num docs: " 
			                           + c + " != " + n);
		}
	}

	public void done() throws PluginException
	{
		System.out.println("encounteredLast=" + encounteredLast );
		System.out.println("DocSourceCheckSink: Total docs received: " + count);
		System.out.println("DocSourceCheckSink: Total chunks doc received: " + docChunksCount);

		checkCount(count.get(), numDocs);
		System.out.println("encounteredLast=" + encounteredLast );

	}

	void extractProperties(MultiValueProp p)
		throws PluginException
	{
		if(!p.containsKey(DocSourceKey))
			{
				throw new PluginException("Source class name: missing sink parameter: \"" 
				                          + DocSourceKey 
				                          + "\": " 
				                          + explainProperties().getProperty(DocSourceKey));
			}
		sourceClassName = p.getProperty(DocSourceKey).get(0);

		if(p.containsKey(NumDocsKey))
			{
				numDocs = Long.parseLong(p.getProperty(NumDocsKey).get(0));			
			}
		sourceProperties = makeSourceProperties(p);
	}

	static final MultiValueProp makeSourceProperties(MultiValueProp p)
	{
		MultiValueProp s = new MultiValueProp();
		Iterator<String>iterator = p.keySet().iterator();
		
		while(iterator.hasNext()){
			String key = iterator.next();
			if(key.equals(DocSourceKey) 
			   || key.equals(NumDocsKey)){
				continue;
			}
			s.put(key, p.get(key));
		}
		return s;
	}
	
	
}
