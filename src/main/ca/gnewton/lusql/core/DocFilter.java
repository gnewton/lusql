package ca.gnewton.lusql.core;
import org.apache.lucene.document.*;

import java.util.*;


/** 
    DocFilter is used to transform a Doc into a Doc.
    It can return the same (perhaps modified) Doc it is passed, 
    or return a different Doc it has created or obtained elsewhere.
    
    In the LuSql process, a DocFilter is after the Doc is populated
    by the DocSource but before it is converted sent to the DocSink
    
    More than one DocFilter can be defined: they are run in the order they 
    are given to LuSql.
    
    Created: Wed Jan 16 02:23:25 2008
    
    @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
    @copyright CISTI / National Research Council Canada
    @version 0.9
    License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
*/

public interface DocFilter 
    extends Plugin
{
    /**
       Apply this filter to the {@link Doc} object.

       @param  doc  the Doc created by the DocSource or previous DocFilter
       @return doc  the filtered Doc. Can be a different Doc from the one passed in
     */
    public Doc filter(Doc doc)
	throws ca.gnewton.lusql.core.FatalFilterException;
    
    /**
       Get the calling {@link LuSql} object. Useful when needing things from the calling LuSql object.
       @return lusql LuSql object
     */
    public LuSql getLuSql();

    /**
       Get the calling {@link LuSql} object. Useful when needing things from the calling LuSql object.
       @return lusql LuSql object
     */
    public void setLuSql(LuSql newLuSql);
}
