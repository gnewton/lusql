package ca.gnewton.lusql.core;

/**
 * Describe class PluginException here.
 *
 *
 * Created: Tue Dec  9 09:36:34 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class PluginException 
    extends Exception
{
	static final long serialVersionUID = 8231166806262105392L;

    public PluginException()
	{
	    super();
	}

    public PluginException(String s)
	{
	    super(s);
	}

    public PluginException(Exception e)
	{
	    super(e);
	}




}
