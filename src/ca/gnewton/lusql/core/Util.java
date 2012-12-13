package ca.gnewton.lusql.core;
import org.apache.lucene.document.*;
import java.util.*;
import java.io.*;
/**
 * Describe class Util here.
 *
 *
 * Created: Mon Jul 21 13:53:12 2008
 *
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 */
public class Util 
    implements LuceneFields
{

    /**
     * Describe out here.
     */
    static private BufferedWriter out = null;

    /**
     * Creates a new <code>Util</code> instance.
     *
     */
    
    private Util() {
	
    }

    public static void msg(String s, boolean error)
  {
      if(LuSql.isVerbose())
	  if(error)
	  System.err.println((error?"Error....":"")
			     + s);
	  else
	      {
		  System.out.println(s);
		  if(out != null)
		      try
		      {
			  out.write(s + "\n");
		      }
		      catch(Throwable t)
		      {
			  t.printStackTrace();
		      }
	      }
  }

    public static String delim(String s)
	{
	    return "[" + s + "]";
	}

  static int getIndex(String s, String[] m)
  {
    for(int i=0; i<m.length; i++)
      if(s.equals(m[i]))
	return i;
    return -1;
  }

//     static LuceneFieldParameters[] makeParameters(Map<String, LuceneFieldParameters> luceneFieldParameters,
// 						  String[] fieldNames,
// 						  String globalLuceneFieldParameters)
	
// 	{
// 	    LuceneFieldParameters[] lpa = new LuceneFieldParameters[fieldNames.length];
// 	    String paras = new String();
// 	    for(int i=0; i<fieldNames.length; i++)
// 	    {
// 		System.out.println("+++++++++ " + fieldNames[i] + " :" + luceneFieldParameters.get(fieldNames[i]));
// 		if(luceneFieldParameters != null && luceneFieldParameters.containsKey(fieldNames[i]))
// 		{
// 		    lpa[i] = luceneFieldParameters.get(fieldNames[i]);
// 		    System.out.println("+++++++++ " + fieldNames[i] + " :" + luceneFieldParameters.get(fieldNames[i]));
// 		}
// 		else
// 		{
// 		    lpa[i] = makeParameters(1,null, globalLuceneFieldParameters)[0];
// 		}
// 	    }
// 	    return lpa;
// 	}





    public static void removeDir(String dir)
	{
	    File d = new File(dir);
	    String[] files = d.list();
	    for(String file: files)
	    {
		File ff = new File(dir, file);
		ff.delete();
	    }
	    d.delete();
	}

    /**
     * Get the <code>Out</code> value.
     *
     * @return a <code>BufferedWriter</code> value
     */
    static public final BufferedWriter getOut() {
	return out;
    }

    /**
     * Set the <code>Out</code> value.
     *
     * @param newOut The new Out value.
     */
    static public final void setOut(final BufferedWriter newOut) {
	out = newOut;
    }
}
