package ca.gnewton.lusql.example;
import org.apache.lucene.document.*;
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.util.zip.*;
import java.util.*;

/**
 * Describe class FileFullTextFilterWriteAllToFile here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */

public class FileFullTextFilterWriteAllToFile 
    extends DBDocFilterImp
{
    // The base directory for all files
    final static String BaseDir = "/mnt/data/dartimin/dartejos/";
    final static String FullTextField = "fulltext";
    final static String FileOutName = "lusqlCat.txt";
    final static int GZIPFACTOR=4;
    BufferedWriter out = null;

    public void init()
	throws ca.gnewton.lusql.core.FatalFilterException
	{
	    System.out.println("FileFullTextFilterWriteAllToFile :: onDone");
	    try
	    {
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(FileOutName), "UTF-8"));
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	}

    public Doc filter(Doc doc)
	throws ca.gnewton.lusql.core.FatalFilterException
	{
	    if(doc == null)
		return null;

	    //The text field (which is the 'rawUrl' field in the db) is the path fragment for the 
	    // fulltext file, which is compressed with gzip;

	    String text="";

	    {
		List<String> fileFields = doc.getFieldValues("text");
		if(fileFields == null)
		{
		    if(doc.getFieldValues("contents") == null)
			return null;
		    text = doc.getFieldValues("contents").get(0);
		    //System.err.println("FileFullTextFilterWriteAllToFile:: problem: no \"text\" field is in the main query");
		    //getLuSql().setFatalError(true);
		    //return doc;
		}
		//else
		//text = readFileToString(BaseDir + fileField, 65536);

		/*
		String fileField = fileFields.get(0);
		if(fileField == null)
		{
		    System.err.println("FileFullTextFilterWriteAllToFile:: problem: no filename field in article id=" +  doc.getFieldValues("id").get(0));
		    return null;
		}
		*/
	    }
	    

	    try
	    {
		//System.out.println(text);
		if(! (text == null || text.length() < 4))
		{
		synchronized("foo")
		{

		    out.write(filterText(text));
		    out.write("\n");
		}
		}
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new ca.gnewton.lusql.core.FatalFilterException();

	    }
	    // doc.addField("contents", filterText(text), textParas);
	    return null;
	}


    String filterText(String s)
	{
	    if(s == null)
		return null;
	    return s.replaceAll("\\n", " ").replaceAll("&#10;", " ").replaceAll("\\r", " ");
	}
    
    Reader makeReader(File f, int bufSize, int estSize)
	throws FileNotFoundException, IOException
	{
	    BufferedReader reader = null;
	    if(f.getName().endsWith(".gz"))
	    {
		reader = new BufferedReader(
		    new InputStreamReader(
			new GZIPInputStream(
			    new FileInputStream(f))), 
		    estSize);
	    }
	    else
	    {
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)), estSize);
	    }
	    return reader;
	}

    String readFileToString(String fileName, int bufSize)
	{
	    Reader reader = null;
	     StringBuilder sb = null;
	    try
	    {
		if(fileName == null)
		    return null;
		File f=null;
		f = new File(fileName);
		if(! (f.exists() && f.canRead()))
		{
		    f = new File(fileName+ ".gz");
		    if(! (f.exists() && f.canRead()))
			{
			    //System.err.println("Unable to read file: " + fileName);
			    return null; // bad!
			}
		}
		// Estimate the ungzipped size for good buffer size estimate
		int estSize;
		if(fileName.endsWith(".gz"))
		    estSize = (int)(f.length() * GZIPFACTOR);
		else
		    estSize = (int)f.length();

		reader = makeReader(f, bufSize, estSize);
		if(reader == null)
		    return "";


		if(bufSize <0)
		    bufSize = estSize;
		sb = new StringBuilder(estSize);
		char[] cbuf = new char[bufSize];
		int n = 0;

		while((n = reader.read(cbuf, 0, bufSize))>0)
		    sb.append(cbuf,0,n);
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	    finally
	    {
		try
		{
		    if(reader!=null)
			reader.close();
		}
		catch(Throwable t)
		{
		    // OK
		}
		
	    }
	    return sb.toString();

	}

    public void onDone()
	throws ca.gnewton.lusql.core.FatalFilterException
	{
	    System.out.println("FileFullTextFilterWriteAllToFile :: onDone");
	    try
	    {
		out.close();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	}
}//////////
