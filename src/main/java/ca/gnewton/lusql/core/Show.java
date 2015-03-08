package ca.gnewton.lusql.core;

import java.lang.reflect.Constructor;
import java.util.*;

public class Show
    implements LuSqlFields
{
    public Show ()
    {

    }

    public static final void main(final String[] args) 
    {
	if(args.length < 1)
	    {
		usage();
	    }
	Show show = new Show();
	show.show(args);
    }

    public static void usage()
    {
	System.out.println("Usage: java ca.gnewton.lusql.core.Show pluginClassName_1 [...pluginClassName_N");
	System.out.println("\tPrint out descriptions and parameters for plugins");
    }
    public void show(final String[] args) 
    {
	for(int i=0; i<args.length; i++)
	    {
		String pluginName = args[i];
		try
		    {
			Class<?> docSourceClass = Class.forName(pluginName);
			Constructor constructor = docSourceClass.getConstructor();
			Plugin pl = (Plugin)constructor.newInstance();
			Properties p = pl.explainProperties();
			output(pluginName, pl);
		    }
		catch(Throwable t)
		    {
			System.out.println("Problem with class: " + pluginName);
			t.printStackTrace();
			break;
		    }
	    }
    }


    void output(String pluginName, Plugin pl)
    {
	System.out.println("\nPlugin Class name: " + pluginName);
	String description = pl.description();
	Properties p = pl.explainProperties();
	System.out.println(" Description: " + (description==null?"none":description));
	printSubInterfaceInto(pl);
	if(p == null)
	    {
		System.out.println("\tNo description available: complain to the plugin implementor!!");
		return;
	    }
	System.out.println(" CLI Settable Properties:");
	int widest = findWidestKey(p);
	Enumeration<?> e = p.propertyNames();
	while(e.hasMoreElements())
	    {
		String key = (String)e.nextElement();
		System.out.println("  " 
				   + key 
				   + makeSpaces(key, widest+1)
				   + p.getProperty(key)
				   );
	    }
    }

    void printSubInterfaceInto(Plugin pl)
    {
	if(pl instanceof DocSink)
	    System.out.println(" Subtype: \"DocSink\" use; \"-" 
			       + CLIDocSinkProperties
			       + "\" to set properties");
	else
	    if(pl instanceof DocSource)
		System.out.println(" Subtype: \"DocSource\"; use \"-" 
			       + CLIDocSourceProperties
			       + "\" to set properties");
	    else
		if(pl instanceof DocFilter)
		System.out.println(" Subtype: \"DocFilter\"; use \"-" 
			       + CLIDocFilterProperties
			       + "\" to set properties");
    }
    
    String makeSpaces(String k, int w)
    {
	String s = new String();
	for(int i=k.length(); i<=w; i++)
	    s += " ";
	return s;
    }

    int findWidestKey(Properties p)
    {
	int widest = 1;
	Enumeration<?> e = p.propertyNames();
	while(e.hasMoreElements())
	    {
		String key = (String)e.nextElement();
		if(key.length() > widest)
		    widest = key.length();
	    }
	return widest;
    }
}