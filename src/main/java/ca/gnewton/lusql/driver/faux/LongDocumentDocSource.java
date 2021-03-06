package ca.gnewton.lusql.driver.faux;

import ca.gnewton.lusql.metadata.*;
import java.lang.annotation.*;
import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;


public class LongDocumentDocSource 
	extends AbstractDocSource 
{
	private final static Logger log = Logger.getLogger(LongDocumentDocSource.class.getName()); 
	static
	{
		BasicConfigurator.configure();
	}
	
	public String description()
	{
		return "Test source that creates 100 documents with fields testField1 and testField2 with integer values 0-100, and 7*(0-100) respectively";
	}
	/**
	 * Creates a new <code>LongDocumentDocSource</code> instance.
	 *
	 */
	public LongDocumentDocSource() {

	}


	@Override
	public final void init(final MultiValueProp properties) 
		throws PluginException 
	{
		
	}

	public final Properties explainProperties() {
		return null;
	}

	public final void done() throws PluginException {

	}

	long i = 0;

	Random r = new Random();
	/**
	 * Describe <code>next</code> method here.
	 *
	 * @return a <code>Doc</code> value
	 * @exception DataSourceException if an error occurs
	 */

	public final static String PrimaryKeyField = "id";
	public final static String SimpleIntField = "intField";
	public final static String FakeTextField = "fakeTextField";
    
	long count = 0;
    
	public final Doc next() throws DataSourceException 
	{
		if(count > numDocs)
			return new DocImp().setLast(true);	
		++count;
	
		Doc doc = new DocImp();
		
		doc.addField(PrimaryKeyField, Long.toString(i));
		doc.addField(SimpleIntField, Long.toString(i*7));
		StringBuilder sb = new StringBuilder();
	
		for(long j=0; j<r.nextInt(7)+1; j++)
			sb.append("word" + r.nextInt(100000) + " ");
		doc.addField(FakeTextField, sb.toString());
	
		i++;
		return doc;
	}

	public final void addField(final String string) {

	}

    
	long numDocs = 10000000000l;

	@PluginParameter(description="Set num docs", optional=true)
	public void setNumDocs(long n)
	{
		System.out.println("setNumDocs: setting from " + numDocs
		                   + " to " + n);
		numDocs = n;
	}
    
	@PluginParameter(description="isThreadSafe", optional=true, isList=true)
	public void addFoo(int s)
	{
	
	}
    

	public boolean isThreadSafe()
	{
		return false;
	}

}
