package ca.gnewton.lusql.metadata;

import java.lang.annotation.*;

@Documented
@Target( {ElementType.METHOD})
@Retention(value=RetentionPolicy.RUNTIME)
public @interface PluginParameter 
{
	String description() default "";
	boolean optional() default true;
	boolean isList() default false;
	// if isList, then 

	
	public class Validator
	{
		static public boolean validMethod(java.lang.reflect.Method method,
		                                  boolean isList)
		{
			boolean foo = isList;
		    
			if(!isList && method.getName().startsWith("set") 
			   ||
			   isList && method.getName().startsWith("add")
			   )
				return true;
			else
				return false;
		}
	}
	    
	    
}
