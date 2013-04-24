package ca.gnewton.lusql.util;

import ca.gnewton.lusql.core.*;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * Describe class Util here.
 *
 *
 * Created: Sun Aug  8 02:06:55 2010
 *
 * @author <a href="mailto:gnewton@chekov">glen</a>
 * @version 1.0
 */
public class Util {

    /**
     * Creates a new <code>Util</code> instance.
     *
     */
    private Util() {

    }

    static public String offset(String s, int n)
    {
	StringBuilder sb = new StringBuilder();
	for(int i=0; i<n; i++)
	    sb.append("  ");
	sb.append(s);
	return sb.toString();
    }

    
    public static Plugin newPlugin(String className)
	throws ClassNotFoundException,
	       NoSuchMethodException,
	       InstantiationException,
	       IllegalAccessException,
	       java.lang.reflect.InvocationTargetException
    {
	Class<?> docSourceClass = Class.forName(className);
	Constructor constructor = docSourceClass.getConstructor();
	return (Plugin)constructor.newInstance();
    }


	public static final boolean isGoodGetUrl(String urlString)
	{
		if(urlString == null){
			throw new NullPointerException("URL is null");
		}
		
		int code = -1;
		HttpURLConnection conn = null;
		try {
			URL url = new URL(urlString);
			conn = (HttpURLConnection) url.openConnection();
			conn.setInstanceFollowRedirects(true);
			conn.setRequestMethod("GET");
			code = conn.getResponseCode();
			if(code == 200){
				return true;
			}
		} 
		catch (Throwable t) 
			{
				t.printStackTrace();
			} 
		finally 
			{
				if (conn != null) {
					conn.disconnect();
				}
			}		
		return false;
	}

	public static final Object instantiateClass(String className)
		throws PluginException
	{
		Class<?> newClass = null;
		try{
			newClass = Class.forName(className);			
		}
		catch(ClassNotFoundException e){
			e.printStackTrace();
			throw new PluginException("Unable to find class: " + className);
		}
		
		Constructor constructor = null;
		try{
			constructor = newClass.getConstructor();
		}
		catch(NoSuchMethodException nsm){
			nsm.printStackTrace();
			throw new PluginException("Unable to find empty constructor for class: " + className);
		}
		Object newInstance = null;
		try{
			newInstance = constructor.newInstance();
		}
		catch(InstantiationException ie){
			ie.printStackTrace();
			throw new PluginException("Unable to instantiate class: " + className);
		}
		catch(java.lang.reflect.InvocationTargetException ite){
			ite.printStackTrace();
			throw new PluginException("Unable to invoke constructor: class: " + className);
		}
		catch(IllegalAccessException iae){
			iae.printStackTrace();
			throw new PluginException("Unable to find access contructor: class: " + className);
		}
		return newInstance;
	}

    
}//
