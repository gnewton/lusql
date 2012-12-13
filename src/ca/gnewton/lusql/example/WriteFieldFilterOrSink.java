package ca.gnewton.lusql.example;
import org.apache.lucene.document.*;
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.util.zip.*;
import java.util.*;
import java.util.concurrent.locks.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class WriteFieldFilter.java here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 * If any of the given fields are null, do not index the document
 */

public class WriteFieldFilterOrSink
    extends AbstractDocSink 
    implements DocFilter
{

    /**
     * Describe FileOutputBase here.
     */
    private String FileOutputBase;

    /**
     * Describe numFiles here.
     */
    private int numFiles;

    /**
     * Describe field here.
     */
    private String field;

    static String FileOutputBaseKey = "fileOutputBase";
    static String NumFilesKey = "numFiles";
    static String FieldKey = "field";

    @Override
    public String description()
	{
	    return "Filter that prints out a particular field and value, spread across N files";
	} 

    ////
    public Properties explainProperties()
	{
	    Properties p = new Properties();
	    p.setProperty(FileOutputBaseKey, "Base filename to write to");
	    p.setProperty(NumFilesKey, "Base filename to write to");
	    p.setProperty(FieldKey, "Field to print out");
	    return p;
	}

    String idFileName = "ids_l.txt";
    Writer[] output = null;
    Random random = null;

    @Override
    public void init(MultiValueProp p) 	
	throws PluginException
	{
	    random = new Random();
	    extractProperties(p);
	    output = new Writer[numFiles];
	    for(int i=0; i<numFiles; i++)
	    {
		try
		{
		    output[i] = new BufferedWriter(new FileWriter(makeOutputFileName(i)));
		}
		catch(Throwable t)
		{
		    t.printStackTrace();
		    throw new PluginException();
		}
	    }
	}

    String makeOutputFileName(int n)
	{
	    return getFileOutputBase() + "_" + n + ".txt";
	}
	

    void extractProperties(MultiValueProp p)
	{
	    if(p.containsKey(FileOutputBaseKey))
		setFileOutputBase(p.getProperty(FileOutputBaseKey).get(0));
	    if(p.containsKey(NumFilesKey))
		setNumFiles(Integer.parseInt(p.getProperty(NumFilesKey).get(0)));
	    if(p.containsKey(FieldKey))
		setField(p.getProperty(FieldKey).get(0));
	}

    int count = -1;
    final Lock l = new ReentrantLock();
    public void addDoc(Doc[] docList)  
	throws DocSinkException
	{
	    for(Doc doc: docList)
		{
		    if(doc.containsField(getField()))
			{
			    try
				{
				    //l.lock();
				    output[getN2()].write(doc.getFieldValues(getField()).get(0) + "\n"); 
				}
			    catch(Throwable t)
				{
				    t.printStackTrace();
				    throw new DocSinkException();
				}
			    finally 
				{
				    //l.unlock();
				}
			}
		    else
			{
			    System.err.println("Not found: " + getField());
			}
		}
	}


	    int getN2()
	{
	    if(numFiles < 2)
		return 0;
	    return random.nextInt(numFiles);
	}
    
    int getN()
	{
	    ++count;
	    if(count >= numFiles)
		count = 0;
	    return count;
	}

    @Override
    public void done() 
	throws PluginException
	{
	    for(int i=0; i<numFiles; i++)
	    {
		if(output[i] != null)
		{
		    try
		    {
			output[i].close();
		    }
		    catch(Throwable t)
		    {
			t.printStackTrace();
			throw new PluginException();
			
		    }
		}
	    }
	}

    /**
     * Get the <code>FileOutputBase</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getFileOutputBase() {
	return FileOutputBase;
    }

    /**
     * Set the <code>FileOutputBase</code> value.
     *
     * @param newFileOutputBase The new FileOutputBase value.
     */
    public final void setFileOutputBase(final String newFileOutputBase) {
	this.FileOutputBase = newFileOutputBase;
    }

    /**
     * Get the <code>NumFiles</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getNumFiles() {
	return numFiles;
    }

    /**
     * Set the <code>NumFiles</code> value.
     *
     * @param newNumFiles The new NumFiles value.
     */
    public final void setNumFiles(final int newNumFiles) {
	this.numFiles = newNumFiles;
    }

    /**
     * Get the <code>Field</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getField() {
	return field;
    }

    /**
     * Set the <code>Field</code> value.
     *
     * @param newField The new Field value.
     */
    public final void setField(final String newField) {
	this.field = newField;
    }

    public int getAddDocSizeHint()
	{
	    return 10;
	}

    public void setPrimaryKeyField(String f)
	{
	    
	}

    public void setRemoveOnDone(boolean b)
	{
	    
	}
    public void commit() throws DocSinkException
	{

	}

    public Object internal()  throws DocSinkException
	{
	    return null;
	}

    public Doc filter(Doc doc)
	throws ca.gnewton.lusql.core.FatalFilterException
	{
	    try 
	    {
		Doc[] dd = new Doc[1];
		dd[0] = doc;
		addDoc(dd);
	    } 
	    catch (DocSinkException t)
	    {
		t.printStackTrace();
		throw new FatalFilterException();
	    }
	    return doc;
	}


    LuSql lusql = null;
    public LuSql getLuSql()
	{
	    return lusql;
	}
    public void setLuSql(LuSql newLuSql)
	{
	    lusql = newLuSql;
	}

}//////////
