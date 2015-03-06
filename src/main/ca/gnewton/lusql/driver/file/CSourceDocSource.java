package ca.gnewton.lusql.driver.file;

import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.util.*;
import java.io.*;

/**
 * Describe class CSourceDocSource here.
 *
 *
 * Created: Fri Apr 17 16:30:11 2009
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class CSourceDocSource 
    extends AbstractDocSource 
{
    public String description()
	{
	    return "Source that reads in CISTI Source files and adds all fields to a Document";
	}

    public final String AN = "AN";
    public final String AO = "AO";
    public final String BD = "BD";
    public final String CC = "CC";
    public final String DA = "DA";
    public final String DE = "DE";
    public final String DO = "DO";
    public final String DT = "DT";
    public final String JN = "JN";
    public final String PG = "PG";
    public final String PY = "PY";
    public final String RE = "RE";
    public final String SF = "SF";
    public final String SN = "SN";
    public final String SO = "SO";
    public final String TI = "TI";
    public final String TN = "TN";
    public final String TP = "TP";
    public final String UD = "UD";
    public final String VI = "VI";
    public final String VN = "VN";
    public final String AB = "AB";

    /**
     * Creates a new <code>CSourceDocSource</code> instance.
     *
     */
    public CSourceDocSource() {

    }

// Implementation of ca.gnewton.lusql.core.Plugin

    /**
     * Describe <code>init</code> method here.
     *
     * @param properties a <code>Properties</code> value
     * @exception PluginException if an error occurs
     */
    
    Scanner scanner = null;
    Doc doc = null;
    
    @Override
    public final void init(final MultiValueProp properties) 
	throws PluginException 
    {
	// open standard in
	try
	    {
		scanner = new Scanner(System.in);
		doc = new DocImp();
		String s = scanner.nextLine();
		doc.addField(s.substring(0,2), s.substring(5));
	    }
	catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
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
	scanner.close();
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
		    if(!scanner.hasNextLine())
			return new DocImp().setLast(true);	
	    
		    String s = null;
	    
		    String prevKey = null;
		    String prevValue = null;
		    while(scanner.hasNextLine() 
			  && ! (s = scanner.nextLine()).startsWith(AN))
			{
			    if(!s.startsWith(" "))
				{
				    if(prevKey != null)
					doc.addField(prevKey, prevValue);			
				    if(s.trim().length() > 5)
					{
					    prevKey = s.substring(0,2);
					    prevValue = s.substring(5).trim();
					}
				}
			    else
				{
				    String tmp = s.trim();
				    if(tmp.length() > 5)
					prevValue += " " + s.substring(5).trim();
				}
			}	    
		    if(prevKey != null)
			doc.addField(prevKey, prevValue);			
		    //c.addField("Test", scanner.nextLine());
		    
		    Doc oldDoc = doc;
		    doc = new DocImp();
		    doc.addField(s.substring(0,2), s.substring(5));
		    doc = oldDoc;

	    return doc;
	    
	}

    /**
     * Describe <code>addField</code> method here.
     *
     * @param string a <code>String</code> value
     */
    public final void addField(final String string) {

    }



}
