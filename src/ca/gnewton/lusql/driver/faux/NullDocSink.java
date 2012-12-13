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
/**
 * Describe class LuceneIndex here.
 *
 *
 * Created: Mon Dec  1 16:09:02 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
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
    
    public void addDoc(Doc[] docList)  
	throws DocSinkException
	{

	}

    public void done()  
	throws PluginException
	{

	}

    public Object internal()
	{
	    return  null;
	}

    public void setPrimaryKeyField(final String newPrimaryKeyField) {

    }

    public boolean isThreaded()
	{
	    return false;
	}
    public boolean isRemoveOnDone()
	{
	    return false;
	}
    public void setRemoveOnDone(boolean b)
	{

	}

    public String descriptor()
	{
	    return this.getClass().getName() ;
	}


}  //////




