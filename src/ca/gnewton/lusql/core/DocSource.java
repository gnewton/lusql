package ca.gnewton.lusql.core;
import java.util.*;

/**
 * Describe interface DataSource here.
 *
 *
 * Created: Wed Dec  3 23:30:44 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public interface DocSource 
    extends Plugin
{
    public Doc next()  throws DataSourceException;
    //public Iterator<Doc> get(String field, String value)
    public void addField(String field);
    public boolean supportsCompression();

    public boolean isSupportsReadingFromStdin();
    public void setReadingFromStdin(boolean b);
    public boolean getReadingFromStdin();
}
