package ca.gnewton.lusql.core;
import java.util.*;
import java.sql.*;
import java.io.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class Doc here.
 *
 *
 * Created: Sat Nov  8 15:29:28 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class DocImp
    implements Doc, Serializable
{
    Map<String, List<String>> fields = null;
    Map<String, List<Reader>> readerFields = null;
    Map<String, File> fileFields = null;
    static Map<String, LuceneFieldParameters> fieldParameters = new HashMap<String, LuceneFieldParameters>();
    /**
     * Creates a new <code>Doc</code> instance.
     *
     */

    public DocImp()
	{
	    init();
	}

    public void populate(ResultSet rs, String[] fieldNames) 
	throws SQLException
	{
	    int nFields = fieldNames.length;
	    for(int i=1; i<=nFields; i++)
	    {
		addField(fieldNames[i-1], rs.getString(i));
	    }
    }


    void init()
	{
	    if(fields == null)
		{
		    fields = new HashMap<String, List<String>>();
		    readerFields = new HashMap<String, List<Reader>>();
		    fileFields = new HashMap<String, File>(1);
		}
	    else
		{
		    fields.clear();
		    fieldParameters.clear();
		    readerFields.clear();
		}
	}

    public void clear()
	{
	    init();
	}
    
    public boolean containsField(String key)
	{
	    return fields.containsKey(key);
	}

    public void removeField(String name)
	{
	    fields.remove(name);
	}

    public Iterator<String> getFieldNames()
	{
	    return fields.keySet().iterator();
	}

    public List<String> getFieldValues(String key)
	{
	    return fields.get(key);
	}

    public List<Reader> getFieldReaders(String key)
	{
	    return readerFields.get(key);
	}
    /*
    public String getFieldValue(String key)
	{
	    return fields.get(key).get(0);
	}
    */
    
    public void addField(final String name, String value)
	{
	    if(value == null)
		return;
	    List<String> values;
	    if(fields.containsKey(name))
		values = fields.get(name);
	    else 
	    {
		values = new ArrayList<String>(); 
		fields.put(name, values);
	    }
	    values.add(value);
	    //addFieldParameter(name, new LuceneFieldParameters());
	}


    public void addField(final String name, Reader reader, LuceneFieldParameters lfp)
	{
	    List<Reader> readers;
	    if(readerFields.containsKey(name))
		readers = readerFields.get(name);
	    else
	    {
		readers = new ArrayList<Reader>();
		readerFields.put(name, readers);
	    }
	    readers.add(reader);
	    addFieldParameter(name, lfp);
	}

    public void addField(final String name, Reader reader)
	{
	    List<Reader> readers;
	    if(readerFields.containsKey(name))
		readers = readerFields.get(name);
	    else
	    {
		readers = new ArrayList<Reader>();
		readerFields.put(name, readers);
	    }
	    readers.add(reader);
	}

    public void addField(final String name, String value, LuceneFieldParameters lfp)
	{
	    addField(name, value, lfp, -999.0f); // magic
	}

    public void addField(final String name, String value, LuceneFieldParameters lfp, float boost)
	{
	    addField(name, value);
	    addFieldParameter(name, lfp);
	    addBoost(name, boost);
	}

    Map<String, Float>fBoost = new HashMap<String, Float>();

    public float getBoost(String field)
	{
	    if(!fBoost.containsKey(field))
		return 1.0f;
	    else
		return fBoost.get(field).floatValue();
	}

	void addBoost(String field, float boost)
	{
	    if(boost < -100.0)
		return;
	    fBoost.put(field, new Float(boost));
	}

    public void addFieldParameter(String field, LuceneFieldParameters paras)
	{
	    fieldParameters.put(field, paras);
	}

    public LuceneFieldParameters getFieldParameters(String field)
	{
	    return fieldParameters.get(field);
	}

    public void addFieldParameters(String[] fieldNames, LuceneFieldParameters[] paras)
	{
	    for(int i=0; i<fieldNames.length; i++)
		addFieldParameter(fieldNames[i], paras[i]);
	}

    public String toString()
	{
	    if(false)
		return "**********";
	    String s = new String("DocImp: toString()\n");
	    Iterator<String>i = fields.keySet().iterator();
	    while(i.hasNext())
	    {
		String key = i.next();
		s+=key+"\n";
		for(String v: fields.get(key))
		{
		    //Iterator<String>l = fields.get(key).iterator();
		    //while(l.hasNext())
		    //s += "\t[" + l.next() + "]\n";
		    s += "\t[" + v + "]\n";
		}
	    }
	    if(s.length() > 400)
		return s.substring(400) + "... - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ";
	    else
		return s + "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ";
	}

    public void setFieldParameters(final Map<String, LuceneFieldParameters> paras)
	{
	    fieldParameters = paras;
	}

    public Map<String, List<String>>getFields()
	{
	    return fields;
	}

    public void setFields(Map<String, List<String>> newFields)
	{
	    fields = newFields;
	}

    public Map<String, LuceneFieldParameters>getParameters()
	{
	    return fieldParameters;
	}

   public void addFields(final String name, List<String>values, LuceneFieldParameters lfp)
	{
	    fields.put(name, values);
	    fieldParameters.put(name, lfp); 
	}

    public void add(Doc doc)
	{
	    if(doc == null)
		return;
	    Iterator<String> it = doc.getFieldNames();
	    while(it.hasNext())
	    {
		String name = it.next();
		this.addFields(name, doc.getFieldValues(name), doc.getFieldParameters(name));
	    }
	}

    boolean last = false;
    public boolean isLast()
    {
	return last;
    }
    public Doc setLast(boolean newLast)
    {
	last = newLast;
	return this;
    }

    public void addFileField(final String name, File value)
    {
	if(fileFields == null)
	    fileFields = new HashMap<String, File>();
	
	fileFields.put(name, value);
    }

    public File getFileField(final String name)
    {
	return fileFields.get(name);
    }
    
    public Iterator<String> getFileFieldNames()
    {
	return fileFields.keySet().iterator();	
    }
    
    public File getFileFieldValue(String key)
    {
	return fileFields.get(key);	
    }
    
    MultiValueProp properties = null;
    public void addProperty(String k, String v)
    {
	if(properties == null)
	    properties = new MultiValueProp();
	properties.add(k,v);
    }
    
    public MultiValueProp getProperties()
    {
	return properties;
    }

    // properties do not have to be equal?
    public boolean sameAs(Doc d)
    {
	if(d == null)
	    return false;

	// Check if same fields
	Iterator<String> names = getFieldNames();
	while(names.hasNext())
	    {
		String name = names.next();
		if( !d.containsField(name) )
		    return false;
	    }
	return true;
    }

    
} ///////////
