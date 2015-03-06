package ca.gnewton.lusql.core;
import java.util.*;


public class DocAdd
    implements Runnable 
{
    //private List<Doc> docs = null;
    private Doc[] docs = null;

    /**
     * Describe docSink here.
     */
    private DocSink docSink = null;

    public DocAdd()
	{

	}

    public final void run() 
	{
	    try
	    {
		getDocSink().addDoc(docs);
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	    
	}

    //List<Doc> getDocs()
    Doc[] getDocs()
	{
	    return docs;
	}

    //public void setDocs(List<Doc> newDocs)    
    public void setDocs(Doc[] newDocs)    
    {
	docs = newDocs;
    }
    

    public void setDoc(Doc newDoc)
	{
	    if(docs == null)
		docs = new Doc[1];
	    docs[0] = newDoc;
	}

    /**
     * Get the <code>DocSink</code> value.
     *
     * @return a <code>DocSink</code> value
     */
    public final DocSink getDocSink() {
	return docSink;
    }

    /**
     * Set the <code>DocSink</code> value.
     *
     * @param newDocSink The new DocSink value.
     */
    public final void setDocSink(final DocSink newDocSink) {
	this.docSink = newDocSink;
    }
}
