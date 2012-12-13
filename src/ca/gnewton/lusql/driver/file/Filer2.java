package gnu.jcp;
// gzip http://groups.google.com/group/envjs/browse_thread/thread/1c1971a3183c8355
//

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.*;

/**
 * Describe class Filer here.
 *
 *
 * Created: Tue Oct 13 20:28:18 2009
 *
 * @author <a href="mailto:gnewton@">glen newton</a>
 * @version 1.0
 */
public class Filer2
    implements Runnable
{
    /**
     * Describe dir here.
     */
    private String dir;

    /**
     * Describe queue here.
     */
    private BlockingQueue<File> queue;

    /**
     * Creates a new <code>Filer</code> instance.
     *
     */
    File rootSrcDir = null;
    File rootDestDir = null;
    public Filer2(String newRootSrcDir, String newRootDestDir) 
	throws NullPointerException
    {
	rootSrcDir = new File(newRootSrcDir);
	if(!rootSrcDir.isDirectory()
	   || !rootSrcDir.canRead()
	   || !rootSrcDir.canExecute())
	    throw new NullPointerException("Problem with directory :"
					   + rootSrcDir);
	if(newRootDestDir != null)
	    rootDestDir = new File(newRootDestDir);
    }


    public void run()
    {
	try
	    {
		dig(rootSrcDir);
		queue.put(new File(""));
	    }
	catch(InterruptedException ie)
	    {
		//ie.printStackTrace();
	    }
    }

    
    File destDir;
    void dig(File ff)
	throws InterruptedException
    {
	
	//	System.out.println("-----> " + ff.getAbsolutePath());
	//	System.out.println(ff.length());
	//	System.out.println("\t" + queue.size());
	if(ff.canRead())
	    {
		System.out.println("zzz " + ff);
		if(!ff.isDirectory())
		    queue.put(ff);
		else
		    {
			File files[] = ff.listFiles();
			if(files == null || files.length == 0)
			    return;
			
			for(int i=0; i<files.length; i++)
			    dig(files[i]);
		    }
	    }
    }

    

    

    /**
     * Get the <code>Dir</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getDir() {
	return dir;
    }

    /**
     * Set the <code>Dir</code> value.
     *
     * @param newDir The new Dir value.
     */
    public final void setDir(final String newDir) {
	this.dir = newDir;
    }

    /**
     * Get the <code>Queue</code> value.
     *
     * @return a <code>BlockingQueue</code> value
     */
    public final BlockingQueue<File> getQueue() {
	return queue;
    }

    /**
     * Set the <code>Queue</code> value.
     *
     * @param newQueue The new Queue value.
     */
    public final void setQueue(final BlockingQueue<File> newQueue) {
	this.queue = newQueue;
    }

    void copyFile(File src)
	throws IOException 
    {
	if(src.isDirectory())
	    handleDirectory(src);
	else
	    copyFile3(src, makeDestFile(src));
	    //copyFile2(src, makeDestFile(src), 64);
	    //copyFile(src, makeDestFile(src));
    }

    
    public void handleDirectory(File src)
	throws IOException, SecurityException
    {
	String rootDest = rootDestDir.getCanonicalPath();
	String rootSrc = rootSrcDir.getCanonicalPath();
	String newDestDir = rootDest + src.getCanonicalPath().substring(rootSrc.length());

	System.out.println("\nDIRECTOPRY \nrootDest=" + rootDest);
	System.out.println("rootSrc=" + rootSrc);
	System.out.println("newDestgDir=" + newDestDir);
	System.out.println("--------> dest=" + newDestDir);

	File newf = new File(newDestDir);
	if(!newf.exists())
	    {
		newf.mkdirs();
		System.out.println("zzz adding dir " + newf);
	    }
	    
    }
    public static void copyFile2(File source, File target, int bufferSize)
	throws IOException 
    {
	FileInputStream in = null;
        FileOutputStream out = null;
        try {
            in = new FileInputStream(source);
            out = new FileOutputStream(target);
            streamOut(in, out, bufferSize);
        } finally {
            if (in != null)
                in.close();
            if (out != null) {
            	// make sure the last buffer is flushed to disk. 
                out.getFD().sync();
                out.close();
            }
        }

    }

    static public void streamOut(InputStream inputStream, OutputStream outputStream, int bufferSize) 
	throws IOException 
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize * 1024);
        ReadableByteChannel inputChannel = Channels.newChannel(inputStream);
        WritableByteChannel outputChannel = Channels.newChannel(outputStream);
        while (inputChannel.read(buffer) != -1) {
            buffer.flip();
            outputChannel.write(buffer);
            buffer.compact();
        }
        buffer.flip();
        while (buffer.hasRemaining()) {
            outputChannel.write(buffer);
        }
    }

    public static void copyFile3(File sourceFile, File destFile) 
	throws IOException 
    {
	if(destFile == null)
	    return;
	FileInputStream is = new FileInputStream(sourceFile);
	if(!destFile.getParentFile().exists())
	    destFile.getParentFile().mkdirs();
	FileOutputStream fos = new FileOutputStream(destFile);
	FileChannel f = is.getChannel();
	FileChannel f2 = fos.getChannel();
	
	f.transferTo(0, f.size(), f2);
	
	f2.close();
	f.close();
    }

    public static void copyFile(File sourceFile, File destFile) 
	throws IOException 
    {
	if(!destFile.exists()) {
	    destFile.createNewFile();
	}
	
	FileChannel source = null;
	FileChannel destination = null;
	try {
	    source = new FileInputStream(sourceFile).getChannel();
	    destination = new FileOutputStream(destFile).getChannel();
	    long count = 0;
	    long size = source.size();
	    while((count += destination.transferFrom(source, 0, size-count))<size);
	    
	}
	finally 
	    {
		if(source != null) {
		    source.close();
		}
		if(destination != null) {
		    destination.close();
		}
	    }
    }


    File makeDestFile(File src)
	throws IOException
    {
	if(rootDestDir == null)
	    return null;
	//String srcPath = src.getCanonicalPath();
	String srcPath = src.getCanonicalPath();
	String rootSrc = rootSrcDir.getCanonicalPath();

	System.out.println("***srcPath=" + srcPath);
	System.out.println("rootSrc=" + rootSrc);
	System.out.println("--- " + srcPath.substring(rootSrc.length()));
	System.out.println("--- " + rootDestDir + srcPath.substring(rootSrc.length()));

	File newf = new File(rootDestDir + srcPath.substring(rootSrc.length()));
	System.out.println("dest canonical=" + newf.getCanonicalPath());
	System.out.println("\tdest parent canonical path=" + newf.getParentFile().getCanonicalPath());
	return newf;
    }
}
