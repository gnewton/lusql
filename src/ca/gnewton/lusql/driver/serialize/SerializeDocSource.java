package ca.gnewton.lusql.driver.serialize;

import java.util.*;
import java.io.*;
import java.util.zip.*;
import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;

public class SerializeDocSource 
    extends AbstractDocSource 
{
    public String description()
	{
	    return "Source that reads serialized LuSql's internal Doc object from disk. Only works with files created with ca.gnewton.lusql.driver.serialize.SerializeDocSink.";
	} 

    private boolean supportsCompression;
    ObjectInputStream oi;

    public SerializeDocSource() {

    }

    public final void init(final MultiValueProp p) throws PluginException 
	{
	    if(!p.containsKey(LuSqlFields.SourceLocationKey))
		throw new PluginException("Missing source file parameter");

	    // Open the environment.
	    try 
	    {
		oi = new ObjectInputStream(
		                           new GZIPInputStream(
		                                               new FileInputStream(p.getProperty(LuSqlFields.SinkLocationKey).get(0))
		                                               ,1024*32)
		                           );
	    } 
	    catch (Throwable t) 
	    {
		t.printStackTrace();
		throw new PluginException("Problem instantiating BDB environment/db");
	    }
	}

    /**
     * Describe <code>explainProperties</code> method here.
     *
     * @return a <code>Map</code> value
     */
    public final Properties explainProperties() {
	return null;
    }

    /**
     * Describe <code>done</code> method here.
     *
     * @exception PluginException if an error occurs
     */
    public final void done() throws PluginException {
	try
	{
	    oi.close();
	}
	catch(Throwable t)
	{
	    t.printStackTrace();
	    throw new PluginException();
	}
    }

// Implementation of ca.gnewton.lusql.core.DocSource

    /**
     * Describe <code>next</code> method here.
     *
     * @return a <code>Doc</code> value
     * @exception DataSourceException if an error occurs
     */
    public final Doc next() throws DataSourceException 
    {
	Doc doc = null;
	try
	    {
		doc = (Doc)(oi.readUnshared());
	    }
	catch(EOFException t)
	    {
		return new DocImp().setLast(true);	
	    }
	
	catch(ClassNotFoundException t)
	    {
		t.printStackTrace();
		throw new DataSourceException();
	    }
	catch(StreamCorruptedException t)
		    {
			t.printStackTrace();
			throw new DataSourceException();
		    }
	catch(OptionalDataException t)
	    {
		t.printStackTrace();
			throw new DataSourceException();
	    }
	catch(ObjectStreamException t)
	    {
		t.printStackTrace();
		throw new DataSourceException();
	    }
	catch(IOException t)
	    {
		t.printStackTrace();
		throw new DataSourceException();
	    }
	catch(Throwable t)
	    {
		t.printStackTrace();
		throw new DataSourceException();
	    }
	return doc;
	
    }

    /**
     * Describe <code>addField</code> method here.
     *
     * @param string a <code>String</code> value
     */
    public final void addField(final String string) {

    }

    /**
     * Get the <code>SupportsCompression</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isSupportsCompression() {
	return supportsCompression;
    }

    /**
     * Set the <code>SupportsCompression</code> value.
     *
     * @param newSupportsCompression The new SupportsCompression value.
     */
    public final void setSupportsCompression(final boolean newSupportsCompression) {
	this.supportsCompression = newSupportsCompression;
    }
}
