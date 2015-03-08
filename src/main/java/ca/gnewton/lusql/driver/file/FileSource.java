package ca.gnewton.lusql.driver.file;

import ca.gnewton.lusql.core.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.*;
import ca.gnewton.lusql.util.*;

public class FileSource
	extends AbstractDocSource
{
	static final public String FilenameField="filename";
	static final public String FileSizeField="filesize";
	static final public String FileLastModifiedField="lastModified";

	static final public String IgnoreDirectoriesKey="dirIgnore";
	boolean ignoreDirectories = true;

	static final public String IncludeSuffixListKey="ext";
	public List<String> includeSuffixes= new ArrayList<String>();

	@Override
	public String description()
	{
		return "Recurses through given directory and finds files and indexes files (path, size, contents)";
	}
    
	ArrayBlockingQueue<File> queue = null; 
	public FileSource()
	{

	}
    
	public boolean requiresPrimaryKeyField()
	{
		return false;
	}

	Thread d = null;

	@Override
	public void init(MultiValueProp p) throws PluginException
	{
		extractProperties(p);
		if(dir == null)
			throw new PluginException("Missing starting directory ("
			                          + LuSqlFields.SourceLocationKey
			                          + ")"
			                          );
		TraverseDirectory tDir = new TraverseDirectory(dir);
		queue = new ArrayBlockingQueue<File>(1);
		tDir.setQueue(queue);
		d = new Thread(tDir);
		d.start();	
	}

	void extractProperties(MultiValueProp p)
	{
		if(p.containsKey(LuSqlFields.SourceLocationKey))
			dir = p.getProperty(LuSqlFields.SourceLocationKey).get(0);

		if(p.containsKey(IgnoreDirectoriesKey))
			ignoreDirectories = Boolean.parseBoolean(p.getProperty(IgnoreDirectoriesKey).get(0));
		if(p.containsKey(IncludeSuffixListKey))
			{
				makeSuffixes(includeSuffixes, p.getProperty(IncludeSuffixListKey).get(0)); 
			}

	}

	void makeSuffixes(List<String> list, String sufs)
	{
		String[] s = sufs.split(",");
		for(int i=0; i<s.length; i++)
			{
				list.add("." + s[i]);
			}
	}



	//start dir
	String dir = null;

	@Override
	public Doc next()
		throws DataSourceException
	{
		File f = null;
		try
			{
				f = queue.take();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
				throw new DataSourceException();
			}
	
		if(f.getName() == "")
			{
				System.err.println("FileSource: TraverseDirectory end");
				return new DocImp().setLast(true);	
			}
		if(f.exists() && f.canRead())
			{
				//System.out.println(" FileSource: " 
				//+ f.getName());
		
				if(f.isDirectory() && !ignoreDirectories)
					return null;
				if(f.isFile())
					{
						//if(!acceptableSuffix(f.getName()))
						//return null;
						//System.err.println("****FileSource: " 
						//+ f.getName());
			
						Doc doc = new DocImp();
			
						doc.addField(FilenameField, 
						             f.getAbsolutePath()
						             //+ File.separator
						             //+ f.getName()
						             );
						doc.addField(FileSizeField, 
						             Long.toString(f.length())
						             //+ File.separator
						             //+ f.getName()
						             );
						doc.addField(FileLastModifiedField,
						             Long.toString(f.lastModified())
						             //+ File.separator
						             //+ f.getName()
						             );

						doc.addFileField("file", f);
						return doc;
					}
			}
	
		return null;
	
		//System.out.println("****************************Returning null");
		//return null;
	}


	boolean acceptableSuffix(String s)
	{
		//System.out.println("Suff size=" + includeSuffixes.size());
		for(int i=0; i<includeSuffixes.size(); i++)
			{
				//System.out.println("Suff=" + includeSuffixes.get(i));
				if(s.endsWith(includeSuffixes.get(i)))
					return true;
			}
		//System.out.println("FileSource: rejecting: " + s);
		return false;
	}

	@Override
	public void addField(String field)
	{

	}

	@Override
	public boolean isThreadSafe()
	{
		return false;
	}

	@Override
	public boolean supportsCompression()
	{
		return false;
	}

	@Override
	public Properties explainProperties()
	{
		Properties newp = new Properties();
		newp.put(IgnoreDirectoriesKey, "Do not include directory entries. Boolean (true, false)");

		return newp;
	}

	@Override
	public void done() throws PluginException
	{
		d.interrupt();
	}

	public static final void main(final String[] args) 
	{
		try
			{
				int count = 0;
				MultiValueProp p = new MultiValueProp();
				p.put(LuSqlFields.SourceLocationKey, "/home/gnewton");
		
				FileSource fs = new FileSource();
				fs.init(p);
				Doc doc = fs.next();
				boolean endd = false;
				while(!endd)
					{
						if(doc.isLast())
							{
								endd = true;
								break;
							}
						System.err.println("* " + count + "  " 
						                   + doc.getFieldValues(FileSource.FilenameField).get(0));
						++count;
						doc = fs.next();
					}
				fs.done();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}
	}
    
}

    

