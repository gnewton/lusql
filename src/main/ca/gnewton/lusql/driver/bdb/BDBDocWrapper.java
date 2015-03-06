package ca.gnewton.lusql.driver.bdb;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.util.*;
import java.io.*;

import java.nio.channels.*;
import java.nio.*;



import ca.gnewton.lusql.core.*;

/**
 * Describe class BDBDocWrapper here.
 *
 *
 * Created: Wed Dec 17 17:40:31 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
@Entity
public class BDBDocWrapper 
{
    private Map<String, List<String>> fields = new HashMap<String, List<String>>();
    private Map<String, List<byte[]>> compressedFields = new HashMap<String, List<byte[]>>();

    private Map<String, byte[]> byteFields = new HashMap<String, byte[]>();
    /*
    String[] fields2 = null;
    List<String[]> values = null;
    */

    /**
     * Describe id here.
     */
    @PrimaryKey
    private String id;

    public BDBDocWrapper()
	{
	    
	}

    /**
     * Get the <code>Id</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getId() {
	return id;
    }

    /**
     * Set the <code>Id</code> value.
     *
     * @param newId The new Id value.
     */
    public final void setId(final String newId) {
	this.id = newId;
    }



    /**
     * Get the <code>Doc</code> value.
     *
     * @return a <code>Doc</code> value
     */
    public final Doc getDoc() {
	return null;
    }

    public Map<String, List<String>> getFields()
	{
	    return fields;
	}
    
    String list(String[] l)
	{
	    String s = new String();
	    for(int i=0; i<l.length; i++)
		s += ": " + l[i];
	    return s;

	}

    public void addDoc(Doc doc)
    {
	Iterator<String> it = doc.getFieldNames();
	while(it.hasNext())
	    {
		String key = it.next();
		List<String>values = doc.getFieldValues(key);
		fields.put(key, values);
	    }

	 it = doc.getFileFieldNames();
	 while(it.hasNext())
	     {
		 String key = it.next();
		 File f = doc.getFileFieldValue(key);
		 byteFields.put(key, getBytesFromFile(f));
	     }
    }

    byte[] getBytesFromFile(File f)
    {
	try
	    {
		if(true)
		    return null;
		
	if(false)
	    {
		Thread.sleep(1);
		return null;
	    }
	
		byte[] b = new byte[10000];
		ReadableByteChannel channel = new FileInputStream(f).getChannel();
		ByteBuffer buf = ByteBuffer.allocateDirect(1024); 
		
		int numRead = 0; 
		int n = 0;
		while (numRead >= 0 && n < 10000) 
		    { 
			// read() places read bytes at the buffer's position so the 
			// position should always be properly set before calling read() 
			// This method sets the position to 0 
			buf.rewind(); 
			// Read bytes from the channel 
			numRead = channel.read(buf); 
			// The read() method also moves the position so in order to 
			// read the new bytes, the buffer's position must be set back to 0 
			buf.rewind(); 
			// Read bytes from ByteBuffer; see also 
			// Getting Bytes from a ByteBuffer  
			for (int i=0; i<numRead; i++) 
			    { 
				b[n++] = buf.get(); 
				if(n > 10000)
				    break;
			    } 
		    }
		return b;
	    }
	catch(Throwable t)
	    {
		return null;
	    }
    }
    
    
}
