package ca.gnewton.lusql.driver.file;
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.locks.*;
import org.apache.log4j.*;

import java.util.concurrent.locks.ReentrantLock;
import ca.gnewton.lusql.driver.file.FileSource;
import java.util.zip.*;
import org.apache.tools.tar.*;
import org.apache.tools.bzip2.*;
import org.apache.tools.ant.taskdefs.Tar.*;
import ca.gnewton.lusql.util.*;


/**
 * Describe class LuceneIndex here.
 *
 *
 * Created: Mon Dec  1 16:09:02 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class TarSink 
    extends AbstractDocSink
{
    public final static String BdbIndexGZIPKey = "indexGzip";
    public final static String BdbIndexBZIP2Key = "indexBzip2";
    public final static String TarFlagKey = "tar";
    public final static String BdbFlagKey = "bdb";
    public final static String BdbDirKey = "bdbDir";
    public final static String CompressKey = "compress";
    public final static String MaxKBytesKey = "maxkbytes";
    
    String bdbDir = "lusql.bdb";

    long maxKBytes = Long.MAX_VALUE;

    boolean bdbIndexGZIP = true;
    boolean bdbIndexBZIP2 = true;
    
    long kbytes = 0l;
    long bytes = 0l;
	//static Category cat = Category.getInstance(LuSql.class.getName());
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public void setPrimaryKeyField(String s)
    {
	// OK
    }

    @Override
    public boolean requiresPrimaryKeyField()
	{
	    return false;
	}

    public String description()
	{
	    return "Sink that stores files in tar format. Designed to use FileSource";
	}
    /**
     * Describe supportsCompression here.
     */
    private boolean supportsCompression = true;

    /**
     * Creates a new <code>LuceneIndex</code> instance.
     *
     */
    public TarSink() {

    }

    int addDocHintSize = 100;
    public int getAddDocSizeHint()
	{
	    return addDocHintSize;
	}

    public void commit() throws DocSinkException
	{
	    try
	    {

	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new DocSinkException();
	    }
	}

    private TarCompressionMethod compression = new TarCompressionMethod();
    TarOutputStream tOut = null;
    BDBFileInfoCore bdb = null;

    ByteCountingOutputStream bcos = null;
    CBZip2OutputStream bzip2OutputStream = null;
    String compress = "none";

    @Override
    public void init(MultiValueProp p) 
	throws PluginException
	{
	    setSupportsWritingToStdout(true);
	    try
	    {
		extractProperties(p);
		if(bdbFlag)
		    {
			bdb = new BDBFileInfoCore(bdbDir, true, false, false);
			if(tarOutName == null)
			    throw new PluginException("Missing tar file name.");
		    }
		if(!tarFlag)
		    return;

		OutputStream outputStream = null;
		if(tarOutName.equals("-"))
		    outputStream = System.out;
		else
		    outputStream = new FileOutputStream(new File(tarOutName));

		outputStream = new ByteCountingOutputStream(outputStream);
		bcos = (ByteCountingOutputStream)outputStream;

		if(compress.equals("bzip2"))
		    {
			outputStream.write('B');
			outputStream.write('Z');
			outputStream = new CBZip2OutputStream(outputStream, 1);
			bzip2OutputStream = (CBZip2OutputStream)outputStream;
		    }
		else
		    if(compress.equals("gzip"))
			outputStream = new GZIPOutputStream(outputStream);			   

		tOut = new TarOutputStream(new BufferedOutputStream(outputStream, 1024 * 4));

		tOut.setLongFileMode(TarOutputStream.LONGFILE_GNU);
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
		throw new PluginException();
	    }
	}

    @Override
    public Properties explainProperties()
	{
	    Properties p = new Properties();
	    p.setProperty(LuSqlFields.BufferSizeKey, "RAM Buffer size");
	    p.setProperty(LuSqlFields.CreateSinkKey, "Create index if does not exist");
	    p.setProperty(LuSqlFields.SinkLocationKey, "Name of index directory");
	    p.setProperty(LuSqlFields.AnalyzerClassKey, "Class name for the analyzer");
	    p.setProperty(LuSqlFields.StopWordFileNameKey, "Name of stop work file");
	    p.setProperty(LuSqlFields.RemoveSinksOnDoneKey, "Remove on done? boolean");
	    return p;
	}

    boolean tarFlag = false;
    boolean bdbFlag = true;

    byte[] buffer = new byte[4 * 1024];    
    TarFileSet tarFileSet = new TarFileSet();

    @Override
    public void addDoc(Doc[] docList)  
	throws DocSinkException
	{
	    
	    try
 	    {
		for(Doc doc: docList)
		    {
			
			String fileName = doc.getFieldValues(FileSource.FilenameField).get(0);
			fileName = fileName.replace(File.separatorChar, '/');
			if(bdbFlag && bdb.isNew && bdb.containsKey(fileName))
			    {
				//System.err.println("TarSink: already stored: fileName");
				return;
			    }

			if(tarFlag)
			    {
				File file = new File(fileName);

				if((bcos.bytes + file.length())/1024l> maxKBytes)
				    {
					LuSql.cleanHalt();
					return;
				    }

				if(compress.equals("bzip2"))
				    bzip2OutputStream.chooseBlockSize(file.length());
				TarEntry te = new TarEntry(fileName);

				te.setModTime(file.lastModified());
				if (!file.isDirectory()) 
				    {
					te.setSize(file.length());
					te.setMode(tarFileSet.getMode());
				    } 
				te.setUserName(tarFileSet.getUserName());
				te.setGroupName(tarFileSet.getGroup());
				te.setUserId(tarFileSet.getUid());
				te.setGroupId(tarFileSet.getGid());
			
				lock.lock(); 
				try
				    {
					tOut.putNextEntry(te);
					FileInputStream fIn = null;
					try
					    {
						if (!file.isDirectory()) 
						    {
							fIn = new FileInputStream(file);
						
							int count = 0;
							do 
							    {
								tOut.write(buffer, 0, count);
								count = fIn.read(buffer, 0, buffer.length);
								bytes += count;
							    } 
							while (count != -1);
						    }
						tOut.closeEntry();
					    }
					finally 
					    {
						if (fIn != null) 
						    {
							fIn.close();
						    }
					    }
				    }
				finally
				    {
					lock.unlock();
				    }
			    }
			if(bdbFlag)
			    {
				BDBFileInfoWrapper w = new BDBFileInfoWrapper();
				try
				    {
					w.setFilePath(fileName);
					bdb.put(w);
					if(bdbIndexGZIP)
					    {
						w.setFilePath(fileName + ".gz");
						bdb.put(w);
					    }
					if(bdbIndexBZIP2)
					    {
						w.setFilePath(fileName + ".bz2");
						bdb.put(w);
					    }
				    }
				catch(Throwable t)
				    {
					t.printStackTrace();
					System.err.println("Error in BDBDocSink.");
					throw new DocSinkException();
				    }
		
			    }
		    }
	    }
	    
	    catch(Throwable t)
		{
		    t.printStackTrace();
		    System.err.println("Error in BDBDocSink.");
		    throw new DocSinkException();
		}
	}
		

    @Override
    public void done()  
	throws PluginException
	{
		//if(LuSql.verbose)
		//cat.info("Closing tar archive");
	    if (tOut != null) 
		{
		    try 
			{
			    // close up
			    tOut.close();
			    System.err.println("KBytes written: " + bytes/1000);
			    if(bdbFlag)
				bdb.done();
			} 
		    catch (IOException e) 
			{
			    // ignore
			}
		}
	    //if(LuSql.verbose)
		    //cat.info("Closed");
	}


    public Object internal()
	{
	    return null;
	}



    public void setRemoveOnDone(final boolean newRemoveOnDone) {
	
    }

    public final boolean isRemoveOnDone() 
    {
	return false;
    }

    @Override
    public boolean isThreaded()
    {
	return false;
    }

    long multi = 1024;
    String tarOutName = null;
    void extractProperties(MultiValueProp p)
    {
	if(p.containsKey(LuSqlFields.SinkLocationKey))
	    tarOutName = p.getProperty(LuSqlFields.SinkLocationKey).get(0);

	if(p.containsKey(BdbIndexGZIPKey))
	    bdbIndexGZIP = Boolean.parseBoolean(p.getProperty(BdbIndexGZIPKey).get(0));

	if(p.containsKey(BdbIndexBZIP2Key))
	    bdbIndexBZIP2 = Boolean.parseBoolean(p.getProperty(BdbIndexBZIP2Key).get(0));

	if(p.containsKey(TarFlagKey))
	    tarFlag = Boolean.parseBoolean(p.getProperty(TarFlagKey).get(0));

	if(p.containsKey(BdbFlagKey))
	    bdbFlag = Boolean.parseBoolean(p.getProperty(BdbFlagKey).get(0));

	if(p.containsKey(BdbDirKey))
	    bdbDir = p.getProperty(BdbDirKey).get(0);

	if(p.containsKey(CompressKey))
	    compress = p.getProperty(CompressKey).get(0);

	if(p.containsKey(MaxKBytesKey))
	    {

		String tmp = p.getProperty(MaxKBytesKey).get(0);
		if(tmp.endsWith("m"))
		    multi = 1024;
		if(tmp.endsWith("g"))
		    multi = 1000*1000;
		maxKBytes = Long.parseLong(tmp.substring(0,tmp.length()-1));
		maxKBytes = maxKBytes * multi;
	    }
    }

}  //////




