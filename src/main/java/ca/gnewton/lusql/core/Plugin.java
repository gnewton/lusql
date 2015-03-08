package ca.gnewton.lusql.core;
import java.util.*;
import ca.gnewton.lusql.util.*;
import org.apache.log4j.Logger;

/**
 Plugin is a dynamically (run-time) loaded module.

 Note that Plugin is incomplete: the method name that is actually run
 is not described. It is assumed that derived interfaces will name and describe
 the specific method that is to be run. Plugin describes all the setup, teardown and 
 ancillary information around a Plugin object, not the method that the Plugin runs.
  
 Created: Tue Dec  9 09:33:32 2008
 
  @author <a href="mailto:gnewton@">Glen Newton</a>
  @version 1.0
 */
public interface Plugin 
{
    /**
       Does all the initialization of the Plugin.
       
       @para initProps a Properties object carries in any init info
    */


    //public void init(Properties initProps) throws PluginException;
    public void init(MultiValueProp p) throws PluginException;

    
    /**
       Describes the Properties that this Plugin can use.
       
       @return a Properties object the the keys being the init property, and the value being a description of the property, with default and example values, if applicable
      */
    public Properties explainProperties();

    /**
       Text description of what this Plugin does.
       
       @return see above
    */
    public String description();

    /**
       After all calls to this Plugin are over, cleans up plugin, releasing resources, etc.
    */
    public void done() throws PluginException;

	public void showState() throws PluginException;
	
	

    // A hint for possible thread issues
    public boolean isThreaded();
    public void setThreaded(boolean b);


    
    // Does LuSql have to wrap its calls to this plugin in a Lock?
    public boolean isThreadSafe();    
    public void setThreadSafe(boolean b);    


	public void setMaxDocs(long l);
}
