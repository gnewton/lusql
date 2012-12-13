package ca.gnewton.lusql.core;
import java.util.*;
 
/**
 * Describe interface Index here.
 *
 *
 * Created: Mon Dec  1 15:55:37 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public interface DocSink
    extends Plugin
{
    //public void addDoc(List<Doc> doc)  throws DocSinkException;
    public void addDoc(Doc[] docs)  throws DocSinkException;
    public Object internal()  throws DocSinkException;

    public boolean isRemoveOnDone();
    public boolean isSupportsCompression();
    public boolean isSupportsWritingToStdout();
    public void setWritingToStdout(boolean b);
    public boolean getWritingToStdout();
    public void commit() throws DocSinkException;
    public void setRemoveOnDone(boolean b);
    public void setPrimaryKeyField(String f);
    public boolean requiresPrimaryKeyField();
    //public int getAddDocSizeHint();    // 2010 08 07
    //public void setCommitFrequency(long n);    // 2009 09 18
    //public void setChunkSize(int n);           // 2009 09 18
}
