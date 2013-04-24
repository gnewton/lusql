package ca.gnewton.lusql.util;

import java.io.*;
import java.util.zip.*;

//import org.apache.commons.io.IOUtils;

public class Compressor{
	public static byte[] compress(byte[] content){
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		GZIPOutputStream gzipOutputStream = null;
		try{
			gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
			gzipOutputStream.write(content);
			gzipOutputStream.close();
		} catch(IOException e){
			throw new RuntimeException(e);
		} finally {
			//byteArrayOutputStream.close();
			//if(gzipOutputStream != null)
			//gzipOutputStream.close();
		}
		
		return byteArrayOutputStream.toByteArray();
	}

	public static byte[] decompress(byte[] contentBytes){
		return decompress(new ByteArrayInputStream(contentBytes));
	}


	public static byte[] decompress(InputStream in){
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPInputStream gin = null;
		
		try{
			//IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(contentBytes)), out);
			gin = new GZIPInputStream(in);
			
			int length = 1024*8;
			byte[] buf = new byte[length];
			while(true){
				int len = gin.read(buf, 0, length);
				if(len == -1){
					break;
				}
				out.write(buf, 0, len);
			}
			
		} catch(IOException e){
			throw new RuntimeException(e);
		}finally{
			//if(gin != null)
			//gin.close();
			//out.close();
		}
		
		return out.toByteArray();
	}

	public static boolean notWorthCompressing(String contentType){
		return contentType.contains("jpeg")
			|| contentType.contains("pdf")
			|| contentType.contains("zip")
			|| contentType.contains("mpeg")
			|| contentType.contains("avi");
	}


	public static void main(String[] args)
	{
		String s = "hello this is a test";

		try{		
			byte[] b = s.getBytes("UTF-8");
			byte[] cb = compress(b);
			byte[] d = decompress(cb);
			String str = new String(d, "UTF-8");
			System.out.println(s);
			System.out.println(str);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		

	}
	
}
