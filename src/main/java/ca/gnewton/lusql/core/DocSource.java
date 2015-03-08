package ca.gnewton.lusql.core;
import java.util.*;


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
