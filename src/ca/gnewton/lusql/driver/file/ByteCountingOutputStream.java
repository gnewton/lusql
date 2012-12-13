package ca.gnewton.lusql.driver.file;

import java.io.*;
import java.util.concurrent.locks.*;

public class ByteCountingOutputStream
	extends OutputStream
{
	private final ReentrantLock lock = new ReentrantLock();
	public int bytes = 0;

	OutputStream os = null;
    
	public ByteCountingOutputStream(OutputStream newInternal)
	{
		os = newInternal;
	}

	public void close()
		throws IOException
	{
		os.close();
		System.err.println("ByteCountingOutputStream kbytes=" + bytes);
	}

	public void flush()
		throws IOException
	{
		os.flush();
	}

	public void write(byte[] b)
		throws IOException
	{
		os.write(b);
		incrementBytes(b.length);
	}

	public void write(byte[] b, int off, int len)
		throws IOException
	{
		os.write(b,off,len);
		incrementBytes(len - off);
	}

	public void write(int b) 
		throws IOException
	{
		os.write(b);
		incrementBytes(1);
	}


	void incrementBytes(int n)
	{
		lock.lock(); 
		try
			{
				bytes += n;
			}
		finally
			{
				lock.unlock();
			}
	}
    
}
