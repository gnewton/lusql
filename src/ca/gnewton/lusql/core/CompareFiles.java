package ca.gnewton.lusql.core;

import ca.gnewton.lusql.util.*;

public class CompareFiles
{
	public static void main(String[] args)
	{
		String file1 = args[0];
		String file2 = args[1];
		
		LuSql lusql = new LuSql();
		
		
		// Source
		lusql.setDocSourceClassName("ca.gnewton.lusql.driver.file.FileLinesSource");
		MultiValueProp p = new MultiValueProp();
		p.setProperty(ca.gnewton.lusql.driver.file.FileLinesSource.FileKey, file1);
		lusql.setSourceProperties(p);
		
		// Sink
		lusql.setDocSinkClassName("ca.gnewton.lusql.driver.bdb.BDBDocSink");
		p = new MultiValueProp();
		lusql.setSinkProperties(p);
		lusql.setPrimaryKeyField(ca.gnewton.lusql.driver.file.FileLinesSource.FileKey);
		lusql.setSinkLocationName("/tmp/foobar4646");

		try{
			lusql.init();
			lusql.run();
		}
		catch(Throwable t){
			t.printStackTrace();
		}
	}
}


