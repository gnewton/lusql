package ca.gnewton.lusql.test;

import java.sql.*;
import java.util.*;

import java.io.*;
import java.util.zip.*;


public class CreateTestDB 
{
	private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocol = "jdbc:derby:";

	static int textSizeInK;
	static int numAuthors;
	static int numArticles;
	static String zipTextFileName;
	static String dbName;
    
	public CreateTestDB() {

	}

	public static void main(String[] args)
	{
		if(args.length == 0 ||
		   args.length != 5)
			{
				usage();
				return;
			}

		dbName = args[0];
		numArticles = Integer.parseInt(args[1]);
		numAuthors = Integer.parseInt(args[2]);
		textSizeInK = Integer.parseInt(args[3]);
		zipTextFileName	= args[4];

		CreateTestDB ctb = new CreateTestDB();
		ctb.run(dbName);
	}

	public void run(String tdb)
		throws NullPointerException
	{

		loadDriver();
		String text = loadText();

		if(text == null)
			return;
		
		Connection conn = null;
		Statement statement = null;
		PreparedStatement pstatement = null;
		try
			{
				try
					{
						conn = DriverManager.getConnection(protocol + tdb
						                                   + ";create=true", null);
					}
		
				catch(java.sql.SQLException t)
					{
						System.err.println("Unable to connect");
						t.printStackTrace();
						throw new NullPointerException();
					}
				conn.setAutoCommit(false);

				//create article table
				statement = conn.createStatement();
				try
					{
						statement.execute("drop table article");
					}
		
				catch(Throwable t)
					{
			
					}
		
				statement = conn.createStatement();
				statement.execute("create table article(id int, title varchar(40), contents CLOB("
				                  + textSizeInK + "K))");
				//populate article table
				pstatement = conn.prepareStatement("insert into article values(?, ?,?)");
				for(int i=0; i<numArticles; i++)
					{
						if(i%10 == 0 && i!= 0)
							System.err.print(".");
						if(i%1000 == 0 && i!= 0)
							System.err.println(" " + i);

						pstatement.setInt(1,i);
						pstatement.setString(2, ("article title value. " + i));
						pstatement.setString(3, getRandomText(text, textSizeInK * 1024));
						pstatement.executeUpdate();
					}
				conn.commit();

				//create author table
				statement = conn.createStatement();
				try
					{
			
						statement.execute("drop table author");
					}
				catch(Throwable t)
					{
			
					}
				statement = conn.createStatement();
				statement.execute("create table author(id int, name varchar(40))");
				//populate author table
				pstatement = conn.prepareStatement("insert into author values(?, ?)");
				for(int i=0; i<numAuthors; i++)
					{
						pstatement.setInt(1,i);
						pstatement.setString(2, ("author name value. " + i));
						pstatement.executeUpdate();
					}
				conn.commit();

				// Create & populate article author join
				Random rand = new Random();
				statement = conn.createStatement();
				try
					{
			
						statement.execute("drop table articleAuthorJoin");
					}
				catch(Throwable t)
					{
			
					}

				statement = conn.createStatement();
				statement.execute("create table articleAuthorJoin(id int, articleId int, authorId int)");
				pstatement = conn.prepareStatement("insert into articleAuthorJoin values(?,?,?)");
				int id = 0;
				for(int i=0; i<numArticles; i++)
					{
						if(i%10 == 0  && i!= 0)
							System.err.print(".");
						if(i%1000 == 0  && i!= 0)
							System.err.println(" " + i);
			
						int nAuthors = 1+rand.nextInt(4);
						Set<Integer>aa = new HashSet<Integer>(4);
						for(int j=0; j<nAuthors; j++)
							{
								int author = rand.nextInt(numAuthors);
								while(true)
									{
										if(aa.contains(author))
											author = rand.nextInt(numAuthors);
										else
											{
												aa.add(new Integer(author));
												break;
											}
									}
								pstatement.setInt(1,id++);
								pstatement.setInt(2,i);
								pstatement.setInt(3, author);
								pstatement.executeUpdate();
							}
			
					}
				conn.commit();
			}
		catch(Throwable t)
			{
				t.printStackTrace();
			}
		finally
			{
				close(statement);
				close(pstatement);
				close(conn);
			}
	
	

	}

	void close(Statement s)
	{
		if (s != null) 
			{
				try
					{
						s.close();
					}
				catch(Throwable t)
					{
						t.printStackTrace();
					}
				finally
					{
						s = null;
					}
			}
	}
    

	void close(Connection conn)
	{

		if (conn != null) 
			{
				try
					{
						conn.close();
					}
				catch(Throwable t)
					{
						t.printStackTrace();
					}
				finally
					{
						conn = null;
					}
			}
	}
    

	void loadDriver()
	{
	
		try 
			{
				Class.forName(driver).newInstance();
			} 
		catch (ClassNotFoundException t) 
			{
				System.err.println("Unable to load the JDBC driver: " + driver);
				System.err.println("CLASSPATH problem?");
				t.printStackTrace(System.err);
			} 
		catch (InstantiationException t) 
			{
				System.err.println(
				                   "Unable to instantiate the JDBC driver " + driver);
				t.printStackTrace(System.err);
			} 
		catch (IllegalAccessException t) 
			{
				System.err.println("Not allowed to access the JDBC driver " + driver);
				t.printStackTrace(System.err);
			}
	}

	String loadText()
	{
		StringBuilder sb = new StringBuilder(10000);
		try
			{
				ZipInputStream in = new ZipInputStream(new FileInputStream(zipTextFileName)); 
				InputStreamReader isr = new InputStreamReader(in);
				BufferedReader br = new BufferedReader(isr);
		
				// Get the first entry 
				ZipEntry entry = in.getNextEntry(); 
				String b = br.readLine();
				while(b != null)
					{
						sb.append(b);
						b = br.readLine();
					}
			}
		catch (Throwable e) 
			{ 
				e.printStackTrace();
				return null;
			} 
		return sb.toString();
	}

	Random rand2 = new Random();
	String getRandomText(String s, int size)
	{
		int l = s.length();
		int start = 1000+rand2.nextInt(s.length() - size-1000);
	
		return s.substring(start, start + size);
	}


	static void usage()
	{
		System.out.println("CreateTestDB DBname numArticles numAuthors testSizeInK zipTextFile");
	}
}
