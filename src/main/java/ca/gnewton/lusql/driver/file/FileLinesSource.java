package ca.gnewton.lusql.driver.file;

import ca.gnewton.lusql.core.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.*;
import ca.gnewton.lusql.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.IOException;

public class FileLinesSource
	extends AbstractDocSource
{
	
	
	public final static String FileKey="file";
	public final static String ContentField="content";

	private String file = null;
	private BufferedReader br = null;
	
	@Override
	public String description()
	{
		return "Reads from a file; each line is the key and is stored";
	}

	@Override
	public void init(MultiValueProp p) throws PluginException
	{
		extractProperties(p);

		if(file == null){
			try{
				br = new BufferedReader(new InputStreamReader(System.in));
			}
			catch(Throwable t){
				t.printStackTrace();
				throw new PluginException();
			}
		}
		else
			{
				File f = new File(file);
				if(!f.exists()){
					throw new PluginException("File does not exist: " + file);
				}
				if(!f.isFile()){
					throw new PluginException("File is not a file: " + file);
				}
				if(!f.canRead()){
					throw new PluginException("File is not readable: " + file);
				}
				try{
					br = new BufferedReader(new FileReader(f));
				}
				catch(Exception e){
					e.printStackTrace();
					throw new PluginException();
				}
			}
	}
	
	void extractProperties(MultiValueProp p)
	{
		if(p.containsKey(FileKey)){
			file = p.getProperty(FileKey).get(0);
		}
	}

	@Override
	public final Doc next() throws DataSourceException
	{
		String line = null;
		try{
			line = br.readLine();
		}
		catch(Exception e){
			e.printStackTrace();
			throw new DataSourceException("Problem reading from BufferedReader");
		}

		Doc doc = new DocImp();
		if(line == null){
			doc.setLast(true);
		}else{
			doc.addField(FileKey, line);
			try{
				doc.addField(ContentField, new BufferedReader(new FileReader(line)));
			}
			catch(Exception e){
				e.printStackTrace();
				throw new DataSourceException(e);
			}
		}
		return doc;
	}

	@Override	
	public final Properties explainProperties() {
		Properties p = new Properties();
		p.setProperty(FileKey, "Full path to file");
		
		return p;
	}
	
}
