package ca.gnewton.lusql.core;

/**
 * Describe interface LocalAnnotation here.
 *
 *
 * Created: Sun Sep 14 17:36:24 2008
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 */

public interface LocalAnnotation 
{
    /**
     * Associates a copyright notice with the annotated API element.
     */
    public @interface Copyright {
        String value();
    }
    
    /**
     * Associates a copyright notice with the annotated API element.
     */
    public @interface License {
        String value();
    }
    
    public @interface Author {
        String value();
    }
    
    @Copyright("2008-2010 National Research Council Canada; 2010 Glen Newton")
    @License("Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt")
    @Author("Glen Newton glen.newton@nrc-cnrc.gc.ca")
    int i = 0;
    } //////
