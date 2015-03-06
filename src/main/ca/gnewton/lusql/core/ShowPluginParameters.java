package ca.gnewton.lusql.core;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Describe class ShowPluginParameters here.
 *
 *
 * Created: Mon Jul 19 16:45:47 2010
 *
 * @author <a href="mailto:gnewton@chekov">glen</a>
 * @version 1.0
 */
public class ShowPluginParameters {

    /**
     * Creates a new <code>ShowPluginParameters</code> instance.
     *
     */
    public ShowPluginParameters() {

    }

    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) 
	throws ClassNotFoundException,
	NoSuchMethodException,
	InstantiationException,
	IllegalAccessException,
	java.lang.reflect.InvocationTargetException
    {
	if(args.length == 0)
	    {
		usage();
		return;
	    }
	for(String plugClassName: args)
	    {
		Class<?> plugClass= null;
		try
		    {
			plugClass = Class.forName(plugClassName);
		    }
		catch(ClassNotFoundException t)
		    {
			System.err.println("Unable to find plugin: "
					   + plugClassName
					   + ". Check CLASSPATH");
			t.printStackTrace();
			continue;
		    }
     
		Constructor constructor = plugClass.getConstructor();
		Plugin plugin = ((Plugin)constructor.newInstance());
		printProperties(plugClassName, plugin);
	    }
	

		   
    }

    static void printProperties(String s, Plugin plugin)
    {
	Properties p = plugin.explainProperties();
	System.out.println("\n- Plugin: " + makeSubType(plugin) + ": " + s);
	int longest = findLongest(p);
	
	Set<Object> keys = p.keySet();
	
	for(Object k: keys)
	    {
		System.out.println("  - " + format(longest,(String)k) + " : " 
				   + formatContent(longest, (String)p.get(k))
				   );
	    }
    }

    static String makeSubType(Plugin p)
    {
	if(p instanceof ca.gnewton.lusql.core.DocSink)
	    return "DocSink";
	if(p instanceof ca.gnewton.lusql.core.DocSource)
	    return "DocSource";
	if(p instanceof ca.gnewton.lusql.core.BaseFilter)
	    return "Filter";
	return "Plugin";
	

    }
    
    static String formatContent(int l, String k)
    {
	StringBuilder sb = new StringBuilder(k.length());
	int lineMax = 80;
	
	int len = k.length();
	int lineSize = 0;
	
	for(int i=0; i<len; i++)
	    {
		if(k.charAt(i) == '\n')
		    lineSize = 0;
		
		if(lineSize>lineMax && k.charAt(i) == ' ')
		    {
			sb.append("\n");
			sb.append(makeSpacing(l));
			lineSize = 0;
		    }
		sb.append(k.charAt(i));
		++lineSize;
	    }
	return sb.toString();
    }

    
    static String makeSpacing(int l)
    {
	l += 6;
	StringBuffer sb = new StringBuffer(l);
	for(int i=0; i<l; i++)
	    sb.append(" ");
	return sb.toString();
    }
    

    static String format(int l, String k)
    {
	StringBuilder sb = new StringBuilder();
	sb.append(k);
	for(int i=k.length(); i<l; i++)
	    sb.append(" ");
	
	return sb.toString();
    }
    

    static int findLongest(Properties p)
    {
	Set<Object> keys = p.keySet();
	int l = 0;
	for(Object k: keys)
	    {
		if(((String)k).length() > l)
		    l = ((String)k).length();
	    }
	return l;
    }
    
    

    static void usage()
    {
	

    }
    


}
