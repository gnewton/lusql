package ca.gnewton.lusql.example;
import org.apache.lucene.document.*;
import java.util.*;
import java.sql.*;
import javax.sql.DataSource;
import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class ExampleDBFilter  here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */
public class ExampleDBFilter  
    extends DBDocFilterImp
{

    @Override
  public Doc filter(Doc doc)
  {
      MultiValueProp p = getProperties();
      //System.out.println(doc);
      String articleId = doc.getFieldValues("id").get(0);
      //System.out.println(articleId);
      
      Connection conn = null;
      Statement stmt = null;
      ResultSet rs = null;

      try
      {
	  conn = getDataSource().getConnection();
	  
	  stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);

	  if(p.get(LuSqlFields.IsMysqlKey).equals("true"))
	      stmt.setFetchSize(Integer.MIN_VALUE);
	  else
	      stmt.setFetchSize(50);
	  
	  rs = null;

	  // Query to get authors 
	  //rs = new ReadOnlyResultSet(stmt, getAuthorsQuery(articleId), 20);
	  rs = new ReadOnlyResultSet(stmt, getAuthorsQuery(articleId));
	  
	  LuceneFieldParameters displayAuParas = new LuceneFieldParameters(Field.Index.NO, Field.Store.YES, Field.TermVector.NO);
	  while(rs.next())
	  {
	      String first = rs.getString(1);
	      String last = rs.getString(2);
	      if(first != null && last != null)
	      {
		  //System.out.println("\t " + rs.getString(1) + ", " + rs.getString(2));
		  String authDisplay = last + ", " + first;

		  doc.addField("displayAu", authDisplay, displayAuParas);
		  /*
	      doc.add(new org.apache.lucene.document.Field("displayAu", authDisplay, 
							   org.apache.lucene.document.Field.Store.YES,
							   org.apache.lucene.document.Field.Index.NO,
							   org.apache.lucene.document.Field.TermVector.NO));
	      

	      String authSearch = last + " " + first;
	      doc.add(new org.apache.lucene.document.Field("searchAu", authSearch, 
							   org.apache.lucene.document.Field.Store.YES,
							   org.apache.lucene.document.Field.Index.TOKENIZED ,
							   org.apache.lucene.document.Field.TermVector.NO));
	      
	      if(last != null)
	      {
		  doc.add(new org.apache.lucene.document.Field("searchAu", last, 
							       org.apache.lucene.document.Field.Store.YES,
							       org.apache.lucene.document.Field.Index.TOKENIZED ,
							       org.apache.lucene.document.Field.TermVector.NO));
		  
		  
		  String authExactSearch = exact(last) + "__" + exact(first);
		  doc.add(new org.apache.lucene.document.Field("exactSearchAu", authExactSearch, 
							       org.apache.lucene.document.Field.Store.YES,
							       org.apache.lucene.document.Field.Index.UN_TOKENIZED,
							       org.apache.lucene.document.Field.TermVector.NO));

	      }
	  }
		  */
	      }
	  }	  
	  rs = new ReadOnlyResultSet(stmt, getKeywordQuery(articleId), 20);
	  while(rs.next())
	  {
	      
	      //System.out.println("\t " + first + ", " + last);
	      String kw = rs.getString(1);
	      addKeyword(doc, kw);

	  }
      }
      catch(Throwable t)
      {
	  t.printStackTrace();
	  return null;
      }
      finally
      {
	  try
	  {
	      if(rs != null)
		  rs.close();
	      if(stmt != null)
		  stmt.close();
	      if(conn != null)
		  conn.close();
	  }
	  catch(Throwable t)
	  {
	      // OK
	  }

      }
      
    return doc;
  }

    // Article, Author  join
    static final String aBase = "select firstName, lastName from Article, ArticleAuthorJoin, Author where Article.id = ArticleAuthorJoin.articleId and ArticleAuthorJoin.authorId = Author.id and Article.id = ";
    String getAuthorsQuery(String articleId)
	{
	    return aBase + articleId;
	}

    // Article, Keyword join
    static final String kwBase = "select string from Article, ArticleKeywordJoin, Keyword where Article.id = ArticleKeywordJoin.articleId and ArticleKeywordJoin.keywordId = Keyword.id and Article.id = ";
    String getKeywordQuery(String articleId)
	{
	    return kwBase + articleId;
	}


    

    String exact(String s)
	{
	    if(s == null)
		return s;
	    return s.replaceAll(" ", "_").replaceAll("'","_").replaceAll("-","_");
	}

    void addKeyword(Doc doc, String kw)
	{
	    List<String> kws = keywordVarients(kw);
	    
	    for(String k: kws)
	    {
		/*
		doc.add(new org.apache.lucene.document.Field("kw", k, 
							     org.apache.lucene.document.Field.Store.YES,
							     org.apache.lucene.document.Field.Index.TOKENIZED,
							     org.apache.lucene.document.Field.TermVector.NO));
		
		doc.add(new org.apache.lucene.document.Field("kwExact", exact(k), 
							     org.apache.lucene.document.Field.Store.YES,
							     org.apache.lucene.document.Field.Index.UN_TOKENIZED,
							     org.apache.lucene.document.Field.TermVector.NO));
		*/
	    }
	}


    Set<String>set = new HashSet<String>();
// See http://www.ssec.wisc.edu/~tomw/java/unicode.html#x0370
    List<String> keywordVarients(String kw)
	{
	    List<String> kws = new ArrayList<String>();
	    kws.add(kw);

	    byte[] b = kw.getBytes();

	    if(
		kw.indexOf("<") >= 0
		&&
		kw.indexOf(">") >= 0
		&& 
		kw.indexOf("<") < kw.indexOf(">")
		)
	    {
		String s = kw.substring(kw.indexOf("<"), kw.indexOf(">")+1);
		if(!set.contains(s))
		    {
			set.add(s);
			//System.out.println("ZZZZ [" + s + "]");
			//System.out.println("ZZZZ [" + kw + "]");
		    }
	    }

	    String nkw = kw;
	    // small greek alpha
	    if(nkw.indexOf("\u03B1")>=0)
	    {
		nkw = nkw.replaceAll("\u03B1", "alpha");
		//System.out.println(nkw);
	    }

	    // small greek beta
	    if(nkw.indexOf("\u03B2")>=0)
	    {
		nkw = nkw.replaceAll("\u03B2", "beta");
		////System.out.println(nkw);
	    }


	    // small greek gamma
	    if(nkw.indexOf("\u03B3")>=0)
	    {
		nkw = nkw.replaceAll("\u03B3", "gamma");
		//System.out.println(nkw);
	    }


	    // small greek delta
	    if(nkw.indexOf("\u03B4")>=0)
	    {
		nkw = nkw.replaceAll("\u03B4", "delta");
		//System.out.println(nkw);
	    }

	    // large greek delta
	    if(nkw.indexOf("\u0394")>=0)
	    {
		nkw = nkw.replaceAll("\u0394", "Delta");
		//System.out.println(nkw);
	    }


	    // prime
	    if(nkw.indexOf("\u2032")>=0)
	    {
		nkw = nkw.replaceAll("\u2032", "'");
		//System.out.println(nkw);
	    }

	    // ----- subscripts

	    // HTML subscript 1
	    if(nkw.toLowerCase().indexOf("<sub>1</sub>")>=0)
	    {
		nkw = nkw.toLowerCase().replaceAll("<sub>1</sub>", "\u8321");
		//System.out.println(nkw);
	    }

	    // HTML subscript 2
	    if(nkw.toLowerCase().indexOf("<sub>2</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>2</sub>", "\u8322");
		nkw = nkw.replaceAll("<SUB>2</SUB>", "\u8322");
		//System.out.println(nkw);
	    }


	    // HTML subscript 3
	    if(nkw.toLowerCase().indexOf("<sub>3</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>3</sub>", "\u8323");
		nkw = nkw.replaceAll("<SUB>3</SUB>", "\u8323");
		//System.out.println(nkw);
	    }

	    // HTML subscript 4
	    if(nkw.toLowerCase().indexOf("<sub>4</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>4</sub>", "\u8324");
		nkw = nkw.replaceAll("<SUB>4</SUB>", "\u8324");
		//System.out.println(nkw);
	    }


	    // HTML subscript 5
	    if(nkw.toLowerCase().indexOf("<sub>5</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>5</sub>", "\u8325");
		nkw = nkw.replaceAll("<SUB>5</SUB>", "\u8325");
		//System.out.println(nkw);
	    }


	    // HTML subscript 6
	    if(nkw.toLowerCase().indexOf("<sub>6</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>6</sub>", "\u8326");
		nkw = nkw.replaceAll("<SUB>6</SUB>", "\u8326");
		//System.out.println(nkw);
	    }

	    // HTML subscript 7
	    if(nkw.toLowerCase().indexOf("<sub>7</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>7</sub>", "\u8327");
		nkw = nkw.replaceAll("<SUB>7</SUB>", "\u8327");
		//System.out.println(nkw);
	    }

	    // HTML subscript 8
	    if(nkw.toLowerCase().indexOf("<sub>8</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>8</sub>", "\u8328");
		nkw = nkw.replaceAll("<SUB>8</SUB>", "\u8328");
		//System.out.println(nkw);
	    }

	    // HTML subscript 9
	    if(nkw.toLowerCase().indexOf("<sub>9</sub>")>=0)
	    {
		nkw = nkw.replaceAll("<sub>9</sub>", "\u8329");
		nkw = nkw.replaceAll("<SUB>9</SUB>", "\u8329");
		//System.out.println(nkw);
	    }







	    // ----- superscripts
	    // HTML superscript 1
	    if(nkw.toLowerCase().indexOf("<sup>1</sup>")>=0)
	    {
		nkw = nkw.replaceAll("<sup>1</sup>", "\u00B9");
		nkw = nkw.replaceAll("<SUP>1</SUP>", "\u00B9");
		//System.out.println(nkw);
	    }

	    // HTML superscript 2
	    if(nkw.indexOf("<sup>2</sup>")>=0)
	    {
		nkw = nkw.replaceAll("<sup>2</sup>", "\u00B2");
		nkw = nkw.replaceAll("<SUP>2</SUP>", "\u00B2");
		//System.out.println(nkw);
	    }

	    
	    // HTML superscript 3
	    if(nkw.indexOf("<sup>3</sup>")>=0)
	    {
		nkw = nkw.replaceAll("<sup>3</sup>", "\u00B3");
		nkw = nkw.replaceAll("<SUP>3</SUP>", "\u00B3");
		//System.out.println(nkw);
	    }
	    
	    
	    /////
	    if(!nkw.equals(kw))
	    {
		kws.add(nkw);
		//System.out.println(kw);
	    }
	    return kws;
	}
}//////////
