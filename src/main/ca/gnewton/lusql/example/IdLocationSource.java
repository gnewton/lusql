package ca.gnewton.lusql.example; 

import ca.gnewton.lusql.core.*;
import java.util.*;
import org.apache.log4j.*;
import java.util.concurrent.locks.*;
import java.util.zip.*;
import java.io.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class IdLocationSource here.
 *
 *
 * Created: Thu Dec  4 15:23:30 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class IdLocationSource
    extends AbstractDocSource
    implements LuSqlFields
{

    public String description()
	{
	    return "Source that gets records from JDBC";
	}
    public Properties explainProperties()
	{
	    Properties p = new Properties();
	    return p;
	}

    //
    BufferedReader input = null;

    @Override
    public void init(MultiValueProp p) throws PluginException
	{
	    extractProperties(p);
	    try
	    {
		input = new BufferedReader(
					   new InputStreamReader(
								 new GZIPInputStream(
										     new FileInputStream("idLocation.txt.gz")
										     )
								 )
					       );
	    }
	    catch(Exception t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	}

    public Doc next()  throws DataSourceException
	{
	    Doc doc = null;
	    
	    try
	    {
		String line = input.readLine();
		if(line == null)
		    {
			done();
			return new DocImp().setLast(true);	
		    }
		else
		    {
			doc = new DocImp();
			String parts[] = line.trim().split(" ");
			doc.addField("id", parts[0]);
			doc.addField("txtUrl", parts[1]);
			//System.out.println(parts[0] + ":" + parts[1]);
			return doc;
		    }
	    }
	    catch(Exception t)
	    {
		done();
		t.printStackTrace();
		throw new DataSourceException("Major SQL problem");
	    }
	}

    public void done()
	{
	    try
	    {

	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }

	    try
	    {

		//if(stmt != null)
		//stmt.close();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	    try
	    {

		//if(conn != null)
		//conn.close();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }

	}


    void extractProperties(MultiValueProp p)
	throws PluginException
	{
	}

    private Set<String>fields = null;
    public void addField(final String field)
	{
	    if(fields == null)
		fields = new HashSet<String>();
	    fields.add(field);
	}

}
