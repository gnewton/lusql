package ca.gnewton.lusql.driver.sparql; 



import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.metadata.*;
import ca.gnewton.lusql.util.Util;
import ca.gnewton.lusql.util.MultiValueProp;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

import java.lang.annotation.*;
import java.util.*;

import org.apache.log4j.*;


public class SparQLDocSource 
	extends AbstractDocSource
	implements LuSqlFields
{
	//static Category cat = Category.getInstance(LuSql.class.getName());

	public static final String SparQLQueryKey = "query";
	public static final String EndPointURLKey = "endPointURL";
	

	ResultSet resultSet = null;
	QuerySolution querySolution = null;
	QueryExecution queryExec = null;

	public final String getQuery() {
		return queryString;
	}

	@Override
	public String description()
	{
		return "Source that gets records from SparQL end-point";
	}

	private String queryString=null;
	private String endPointUrl=null;
	private Set<String>fields = null;

	@Override
	public Properties explainProperties()
	{
		Properties p = new Properties();
		p.setProperty(SparQLQueryKey, "SPARQL query to apply");
		p.setProperty(EndPointURLKey, "End point URL");
		return p;
	}

	//
	@Override
	public void init(MultiValueProp p) throws PluginException
	{
		extractProperties(p);
	    
		//cat.info("SPARQL query=" + queryString);
		//cat.info("End point URL" + endPointUrl);
		if(!Util.isGoodGetUrl(endPointUrl)){
			throw new PluginException("Unable to connect to URL with GET: " +  endPointUrl);
		}
		
		try
			{
				Query query = QueryFactory.create(queryString);
				queryExec = QueryExecutionFactory.sparqlService(endPointUrl, query);

				resultSet = queryExec.execSelect();
				querySolution = resultSet.next();

				List<String> fieldNamesList = resultSet.getResultVars();

				fieldNames = new String[fieldNamesList.size()];
				for(int i=0; i<fieldNames.length; i++){
					fieldNames[i] = fieldNamesList.get(i);
				}
				
			}
		catch(Throwable t)
			{
				done();
				//cat.error("Exception creating query");
				t.printStackTrace();
				throw new PluginException();
			}
	}

	@Override
	public Doc next()  throws DataSourceException
	{
		Doc doc = null;
		try
			{
				if(!resultSet.hasNext()){
					doc = new DocImp().setLast(true);	
				}else{
					doc = makeDoc(querySolution);
					querySolution = resultSet.next();
				}
			}
		catch(Throwable t)
			{
				done();
				t.printStackTrace();
				throw new DataSourceException("Major SQL problem");
			}
		return doc;
	}

	int nFields;
	String[] fieldNames;

	@Override    
	public void done()
	{
		if(queryExec != null){
			queryExec.close();
		}
		queryExec = null;
		resultSet = null;
		//cat.info("Done");
	}

	Doc makeDoc(QuerySolution qs)
		throws DataSourceException
	{
		Doc d = new DocImp();
		try
			{
				populate(d, qs, fieldNames);
			}
		catch(Exception t)
			{
				done();
				t.printStackTrace();
				throw new DataSourceException();
			}
		return d;
	}

	public void populate(Doc d, QuerySolution qs, String[] fieldNames) 
	{
		int nFields = fieldNames.length;
		for(int i=0; i<nFields; i++)
			{
				d.addField(fieldNames[i], qs.get(fieldNames[i]).toString());
			}
	}



	void extractProperties(MultiValueProp p)
		throws PluginException
	{
		if(!p.containsKey(SparQLQueryKey))
			{
				throw new PluginException("Missing query");
			}
		if(!p.containsKey(EndPointURLKey))
			{
				throw new PluginException("Missing end point url");
			}

		queryString = p.getProperty(SparQLQueryKey).get(0);
		endPointUrl = p.getProperty(EndPointURLKey).get(0);
	}




	public void addField(final String field)
	{
		if(fields == null)
			fields = new HashSet<String>();
		fields.add(field);
	}

	public static final void main(final String[] args) 
	{
		/*
		DocSource ds = new JDBCDocSource();
		MultiValueProp p = new MultiValueProp();
		p.setProperty(QueryKey, "select * from Article limit 100");
		p.setProperty(DBUrlKey, "jdbc:mysql://blue01/dartejos?user=gnewton&password=GNewton&autoReconnectForPools=true&cacheResultSetMetadata=true");
		p.setProperty(JDBCDriverKey, "com.mysql.jdbc.Driver");
		p.setProperty(JDBCFetchSizeKey,"1");

		try
			{
				ds.init(p);
			}
		catch(PluginException t)
			{
				t.printStackTrace();
			}
	
		Doc d = null;
		try
			{
				while((d = ds.next()) != null)
					{
						System.out.println(d);
					}
				ds.done();
			}
		catch(DataSourceException t)
			{
				t.printStackTrace();
			}
		catch(PluginException t)
			{
				t.printStackTrace();
			}
		*/
	}
	

}
