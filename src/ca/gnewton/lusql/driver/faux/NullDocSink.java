package ca.gnewton.lusql.driver.faux;
import java.io.*;
import java.util.Properties;
import org.apache.lucene.index.*;
import org.apache.lucene.analysis.Analyzer;
import java.lang.reflect.Constructor;
import org.apache.lucene.document.*;
import org.apache.lucene.store.*;
import java.util.*;
import java.util.concurrent.locks.*;

import ca.gnewton.lusql.util.*;
import ca.gnewton.lusql.core.*;

public class NullDocSink 
    extends AbstractDocSink
{

    public String description()
	{
	    return "Test sink that does nothing.";
	}

    /**
     * Creates a new <code>LuceneIndex</code> instance.
     *
     */
    public NullDocSink() {

    }

    int addDocHintSize = 100;
    public int getAddDocSizeHint()
	{
	    return addDocHintSize;
	}

    public void commit() throws DocSinkException
	{

	}


    @Override
    public void init(MultiValueProp p) 
	throws PluginException
	{
	    setSupportsWritingToStdout(false);
	}

    public Properties explainProperties()
	{
	    // FIXX
	    return null;
	}
	
	long count = 0l;
	public void addDoc(Doc[] docList)  
	throws DocSinkException
	{
		count += (long)(docList.length);
	}

    public void done()  
	throws PluginException
	{
		System.out.println();
		System.out.println("Num docs=" + count);
		System.out.println();
		
	}

    public Object internal()
	{
	    return  null;
	}

    public boolean isThreaded()
	{
	    return false;
	}
    public boolean isRemoveOnDone()
	{
	    return false;
	}

    public String descriptor()
	{
	    return this.getClass().getName() ;
	}


}  //////




