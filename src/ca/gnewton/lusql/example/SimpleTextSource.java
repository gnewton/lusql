package ca.gnewton.lusql.example; 

import ca.gnewton.lusql.core.*;
import java.util.*;
import org.apache.log4j.*;
import java.util.concurrent.locks.*;
import java.util.zip.*;
import java.io.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class SimpleTextSource here.
 *
 *
 * Created: Thu Jul 8 15:23:30 2010
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class SimpleTextSource
    extends AbstractDocSource
    implements LuSqlFields
{

    public String description()
	{
	    return "Trivial Source that creates 30 records with field 'value=\"1\"..\"30000\"'";
}
public Properties explainProperties()
	{
	    Properties p = new Properties();
	    return p;
	}

    //
    int count = 0;
    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    extractProperties(p);
	}

    public Doc next()  throws DataSourceException
	{
	    Doc doc = new DocImp();
	    count++;
	    
	    doc.addField("value", "a");
	    if(count > 30000)
		{
		    return new DocImp().setLast(true);
		}
	    return doc;
	}


    public void done()
	{
	    
	}


    void extractProperties(MultiValueProp p)
	throws PluginException
	{
	}

    public void addField(final String field)
    {

    }
}
