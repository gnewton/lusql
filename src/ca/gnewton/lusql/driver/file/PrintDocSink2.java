package ca.gnewton.lusql.driver.file;
import ca.gnewton.lusql.core.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class PrintDocSink2 here.
 *
 *
 * Created: Sat Dec 20 23:07:13 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class PrintDocSink2 
    extends AbstractDocSink
{
    int mantissa = 100000;
    boolean link = false;
    int filesPerDir = 1500;

    @Override
    public String description()
	{
	    return "Sink that prints out the documents and their fields/values. Useful for validation of Documents/fields";
	}

    @Override
    public boolean requiresPrimaryKeyField()
	{
	    return false;
	}
    /**
     * Describe indexName here.
     */
    private String indexName;

    int addDocHintSize = 100;
    public int getAddDocSizeHint()
	{
	    return addDocHintSize;
	}

    /**
     * Creates a new <code>PrintDocSink2</code> instance.
     *
     */

    public PrintDocSink2() 
	{

	}


    OutputStream out = null;
    public void init(MultiValueProp p) throws PluginException
	{
	    setSupportsWritingToStdout(true);
	    try
		{
		    out = new BufferedOutputStream(
						   new GZIPOutputStream(
									new FileOutputStream("joscp.sh.gz")));
		    out.write(("b=" + base + "\n").getBytes());
		}
	    catch(Throwable t)
		{
		    t.printStackTrace();
		    throw new PluginException("Cannot open file");
		}
	}

    public Properties explainProperties()
	{
	    return null;
	}

    public void done() throws PluginException
	{
	    try
	    {
		if(out != null)
		    out.close();
	    }
	    catch(Throwable t)
	    {
		throw new PluginException();
	    }

	}

    // orca 
    String base = "/mnt/blue01/dartimin/dartejos";
    // blues
    //String base = "/mnt/data/dartimin/dartejos";

    int count = 0;
    public void addDoc(Doc[] docList)  throws DocSinkException
	{
	    try 
		{
		    l.lock();
		    for(Doc doc: docList)
			{
			    
				String src =  doc.getFieldValues("txtUrl").get(0);
			    src = src.replace(";", "\\;");
			    String dest = doc.getFieldValues("id").get(0) + ".txt";
			    
			    try 
				{
				    out.write(makeBash(src,dest).getBytes());
				}
			    finally 
				{
				    l.unlock();
				}
			}
		}
	    catch(Throwable t)
		{
		    t.printStackTrace();
		    throw new DocSinkException();
		}
	}
    
    Lock l = new ReentrantLock();    
    int cdCount = 0;
    public String makeBash(String s, String d)
    {
	String p = "&";
	if(count %10 == 0)
	    p = "";
	//p = "";
	
	String cd = "";

	if(count %filesPerDir == 0)
	    {
		if(cdCount != 0)
		    cd = "cd ..\n";
		cd += "mkdir " + makeDir(cdCount) + "\n" + "cd " + makeDir(cdCount) + "\n";
		++cdCount;
	    }
	++count;
	StringBuffer sb = new StringBuffer("#\n");
	sb.append(cd);
	sb.append("s=${b}/" + s + "\n");
	sb.append("d=" + d + "\n");
	sb.append("if [[ -e ${s}.gz" + " ]]\n");
	sb.append("then\n");
	if(link)
	    sb.append("\t ln -s ${s}.gz $d.gz" + p + "\n");
	else
	    sb.append("\t cp ${s}.gz $d.gz" + p + "\n");
	sb.append("else \n");
	sb.append("\t if [[ -e $s ]]\n");
	sb.append("\t then\n");
	if(link)
	    sb.append("\t\t ln -s $s $d" + p + "\n");
	else
	    sb.append("\t\t cp $s $d" + p + "\n");
	sb.append("\t fi \n");
	sb.append("fi \n");
	return sb.toString();
    }


    public String makeDir(int c)
    {
	++c;
	StringBuffer sb = new StringBuffer();
	int d = mantissa;
	while(d>c)
	    {
		sb.append("0");
		d /= 10;
	    }
	sb.append(c);
	System.out.println(c + ": " + sb);
	return sb.toString();
    }

    public Object internal()  throws DocSinkException
	{
	    return null;
	}

    public boolean isThreaded()
	{
	    return false;
	}

    public boolean isRemoveOnDone()
	{
	    return false;
	}

    public void commit() throws DocSinkException
	{

	}
    public void setRemoveOnDone(boolean b)
	{

	}
    public void setPrimaryKeyField(String f)
	{

	}

    void extractProperties(Properties p)
	{
	    if(p.containsKey("index"))
		setIndexName(p.getProperty("index"));
	}

    /**
     * Get the <code>IndexName</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getIndexName() {
	return indexName;
    }

    /**
     * Set the <code>IndexName</code> value.
     *
     * @param newIndexName The new IndexName value.
     */
    public final void setIndexName(final String newIndexName) {
	this.indexName = newIndexName;
    }

    public String descriptor()
	{
	    return this.getClass().getName() + ": File: " + getIndexName();
	}
}
