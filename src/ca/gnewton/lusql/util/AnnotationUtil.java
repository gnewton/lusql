package ca.gnewton.lusql.util;

import ca.gnewton.lusql.core.Plugin;
import ca.gnewton.lusql.core.PluginException;
import ca.gnewton.lusql.metadata.PluginParameter;

public class AnnotationUtil
{
	public static final void handleAnnotations(Class klass)
		throws PluginException
	{
		for(java.lang.reflect.Method method : klass.getDeclaredMethods()) 
			{
				PluginParameter paramAnnot = method.getAnnotation(PluginParameter.class);
				if(paramAnnot != null)
					{
						if(!PluginParameter.Validator.validMethod(method, paramAnnot.isList()))
							if(!paramAnnot.isList())
								throw new PluginException("Illegal method for annotation: "
								                          + " Annotation: " 
								                          + PluginParameter.class.getName()
								                          + "; method: "
								                          + klass.getName()
								                          + "." 
								                          + method.getName()
								                          + "; only to be used on get'ters (\"getFoo\")."
								                          );
							else
								throw new PluginException("Illegal method for annotation: "
								                          + " Annotation: " 
								                          + PluginParameter.class.getName()
								                          + "; method: "
								                          + klass.getName()
								                          + "." 
								                          + method.getName()
								                          + "; only to be used on add'ers (\"addFoo\")."
								                          );
			    
						System.out.println("Method=" + method);
						System.out.println("MethodName=" + method.getName());
						System.out.println("\t" + paramAnnot);
						System.out.println("\t description: " + paramAnnot.description());
						System.out.println("\t optional: " + paramAnnot.optional());
						// fixxx xxx
						/*
						  if(allPluginProps.containsKey(getDocSourceClassName()))
						  {
						  MultiValueProp mvp = new 
						  Object o = method.invoke(source, new Integer(100));
			    
						  System.out.println("\t\t" + o);
						*/
			    
					}
		    
				/*
				  HiddenCreoleParameter hiddenParamAnnot = method
				  .getAnnotation(HiddenCreoleParameter.class);
				  if(paramAnnot != null || hiddenParamAnnot != null) {
				  if(!method.getName().startsWith("set") || method.getName().length() < 4
				  || method.getParameterTypes().length != 1) {
				  throw new GateException("Creole parameter annotation found on "
				  + method
				  + ", but only setter methods may have this annotation.");
				*/
			}

		//Annotation[] anns = klass.getAnnotation(a);
		//for(Annotation an: anns)
		{
			//System.out.println("76567--------------- " + an);
		    
		}
	    
	    



	}
	


}
