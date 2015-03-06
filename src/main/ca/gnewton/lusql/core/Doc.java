package ca.gnewton.lusql.core;
import java.util.*;
import java.sql.*;
import java.io.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe interface Doc here.
 *
 *
 * Created: Mon Nov 10 16:46:22 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public interface Doc 
{
    public void clear();
    public boolean containsField(String key);
    public void removeField(String name);
    public Iterator<String> getFieldNames();
    public Map<String, List<String>>getFields();
    public Map<String, LuceneFieldParameters>getParameters();
    public List<String> getFieldValues(String key);
    public List<Reader> getFieldReaders(String key);
 
    public void add(Doc doc);

    public void setFields(Map<String, List<String>> fields);
    public void addFields(final String name, List<String>values, LuceneFieldParameters lfp); 
    public void addField(final String name, String value, LuceneFieldParameters lfp);
    public void addField(final String name, Reader reader, LuceneFieldParameters lfp);
    public void addField(final String name, Reader reader);
    public void addField(final String name, String value, LuceneFieldParameters lfp, float boost);
    public float getBoost(String Field);
    public void addFieldParameters(String[] fieldNames, LuceneFieldParameters[] paras);
    public void setFieldParameters(Map<String, LuceneFieldParameters> paras);
    public LuceneFieldParameters getFieldParameters(String field);
    public void addField(final String name, String value);

    public void addFileField(final String name, File value);
    public Iterator<String> getFileFieldNames();
    public File getFileFieldValue(String key);
    
    //public void addField(final String name, byte[] value, LuceneFieldParameters lfp);
    public void addFieldParameter(String field, LuceneFieldParameters paras);

    // Needs to be removed!!!!!!!
    public void populate(ResultSet rs, String[] fieldNames) throws SQLException;

    public boolean isLast();
    public Doc setLast(boolean l);

    public void addProperty(String k, String v);
    public MultiValueProp getProperties();

    public boolean sameAs(Doc d);
    
}
