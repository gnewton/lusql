package ca.gnewton.lusql.example;
import org.apache.lucene.document.*;
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.util.zip.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Describe class FieldNotNullFilter here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 * @author <a href="mailto:glen.newton@gmail.com">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 * If any of the given fields are null, do not index the document
 */

public class FieldNotNullFilter 
    extends DBDocFilterImp
{
    //HashSet<String>one = new HashSet<String>(100);
    //HashSet<String>two = new HashSet<String>(100);
    Map<String,String>one = new ConcurrentHashMap<String,String>(100);
    Map<String,String>two = new ConcurrentHashMap<String,String>(100);
    final static String[] notNull = {"contents", "path", "filename"};

    final static String[] badTitles = 
    {
	"* editorial board",
	"1",
	"2",
	"3",
	"75 years",
	"abstract(s)",
	"abstracts",
	"acknowledgement of references",
	"acknowledgement of reviewers",
	"acknowledgements",
	"acknowledgementtoreferees",
	"acknowledgment",
	"ad",
	"ad-contents direct",
	"addendum",
	"addendum/corrigendum",
	"agenda",
	"aims & scope",
	"annonce",
	"annoucement",
	"announcements",
	"annual meeting",
	"appendices",
	"article reviewers",
	"auteurs/authors",
	"auther index",
	"author index",
	"author index",
	"author keyword",
	"author",
	"author's index",
	"author's reply",
	"author/title index",
	"authors index",
	"authors' index",
	"bereavement",
	"biographical notes",
	"board of editors",
	"book received",
	"book review",
	"book review(s)",
	"book reviews",
	"book reviews",
	"books received",
	"books/livres",
	"calendar",
	"call for nominations",
	"classifed advertising",
	"classified ads",
	"classified adverstising",
	"classified advertising",
	"closing comments",
	"closing comments",
	"closure",
	"college news",
	"college news",
	"coming attractions",
	"communication",
	"comptes%gâ%@rendus",
	"concluding comment",
	"conference paper(s)",
	"conference reports",
	"conmtentsdirect",
	"content, author index",
	"contents contd.",
	"contents continued",
	"contents continued",
	"contents index",
	"contents index",
	"contents list",
	"contents of volume",
	"contents page",
	"contents volume *",
	"contents volume",
	"contents",
	"contents/abstracts",
	"contributions",
	"correction",
	"correction",
	"correspondence",
	"corrigenda",
	"corrigendum",
	"cumulative index",
	"dear editor",
	"discussion",
	"ed. board",
	"editoeial board",
	"editor",
	"editor's introduction",
	"editor's message",
	"editor's note",
	"editor's note",
	"editor's reply",
	"editorial advisory board",
	"editorial announcement",
	"editorial board",
	"editorial board",
	"editorial board/contents",
	"editorial board/publication info",
	"editorial board/publication information",
	"editorial board/publication",
	"editorial borad",
	"editorial note",
	"editorial note",
	"editorial statement",
	"editorial",
	"editorial(s)",
	"editorials",
	"editors",
	"editors' note",
	"errata",
	"errata/corrige",
	"erratum",
	"erratum/attention librarians!",
	"executive board",
	"executive summaries",
	"expositions",
	"film reviews",
	"foreword",
	"forthcoming events",
	"forthcoming paper",
	"forthcoming papers",
	"forthcoming papers",
	"forthcoming publications",
	"forthcomingpapers",
	"full issue",
	"full issue",
	"full title page",
	"future articles",
	"general instructions",
	"guest editorial",
	"guide for authors",
	"guide for authors",
	"guideforauthors",
	"i-stat",
	"ictp2008",
	"in memoriam",
	"index by domain",
	"index des announceurs",
	"index of advertisers",
	"index",
	"index-author",
	"indexes",
	"information",
	"inside front cover*",
	"inst to authors",
	"instructions to authors",
	"insts to authors",
	"insts to authors",
	"introductory comments",
	"invited response",
	"issues & opinions ",
	"journal officiel",
	"keyword index",
	"keywords",
	"keywords",
	"l'agenda",
	"le journal officiel",
	"lifestyles",
	"list of contents",
	"list of reviewers",
	"list of reviewers*",
	"literature alert",
	"looking ahead",
	"léditorial",
	"masthead",
	"media review",
	"meeting schedule",
	"miscellaneous",
	"mots cles",
	"name index",
	"national congress",
	"new books",
	"new publication",
	"new relations",
	"new subscription rates",
	"news and announcements",
	"news briefs",
	"news item",
	"news items",
	"newsletter",
	"note de l'éditeur",
	"notice board",
	"noticeboard",
	"obituary",
	"offres d'emplol",
	"online submission",
	"other contents",
	"patent alerts",
	"patent reports",
	"patent report(s)",
	"personal report",
	"personal report",
	"poster sessions",
	"preliminary announcement",
	"president's page",
	"presidents'page",
	"press release",
	"problem page",
	"problems page",
	"product review(s)",
	"profile",
	"prologue",
	"présentation/presentation",
	"publication overview",
	"publisher note",
	"publisher'a note",
	"publisher's acknowledgement",
	"publisher's acknowledgement",
	"publisher's announcement",
	"publisher's announcement",
	"publisher's information pages",
	"publisher's note",
	"publisher's note",
	"publisher's note/atl>",
	"publishers note",
	"readers' comments",
	"received book",
	"request(s) for assistance",
	"research papers",
	"response",
	"review",
	"reviewer list",
	"reviewers list",
	"science reviews",
	"sommaire",
	"special announcements",
	"special issue contents",
	"special issue page",
	"subject index",
	"subject index",
	"subscription",
	"symposia",
	"table of contents",
	"taxonomic index",
	"textbook error",
	"the authors reply",
	"title index",
	"title index",
	"title page",
	"title section",
	"title",
	"treasurer's report",
	"update",
	"vbac",
	"video review",
	"volume * contents",
	"volume * index",
	"volume 5",
	"volume contents",
	"volume contents",
	"volume contents",
	"volume index",
	"volume index",
	"volume index",
	"welcome",
	"éditorial"
    };

    Set bt = new HashSet(badTitles.length);
    public void init()
	{
	    for(int i=0; i<badTitles.length; i++)
		bt.add(badTitles[i]);
	}

    int badCount = 0;
    public Doc filter(Doc doc)
	{
	    if(doc == null)
		return null;

	    for(int i=0; i<notNull.length; i++)
		{
		    List<String> fileFields = doc.getFieldValues(notNull[i]);
		    if(fileFields == null)
			return null;
		    else
		    {
			if(i==0)
			for(String value:fileFields)
			{
			    if(value != null && bt.contains(value.trim().toLowerCase()))
			    {
				++badCount;
				return null;
			    }
			    if(value != null && value.trim().length() <1)
				return null;
			}
			if(i==0 && false)
			{
			    for(String value:fileFields)
			    {
				if(value ==null)
				    continue;
				if(bt.contains(value.trim().toLowerCase()))
				    {
					System.out.print("*");
					continue;
				    }
				String[] parts = value.trim().split(" ");
				if(parts.length == 1)
				    one.put(value.trim(), "");

				else
				    if(parts.length == 2)
				    two.put(value.trim(), "");
				/*
				else
				    if(parts.length == 3)
					three.add(value);
				*/
			    }
			    if(fileFields.size() == 0)
				return null;
			}
		    }
		    
		}
	    return doc;
	}

    public void onDone()
	{
	    System.out.println("Bad count = " + badCount);
	    /*
	    Iterator<String>it = one.keySet().iterator();
	    System.out.println("\n1111111111111111");
	    while(it.hasNext())
	    {
		String key = it.next();
		if(key.trim().length() > 0)
		    System.out.println(key);
	    }

	    it = two.keySet().iterator();
	    System.out.println("\n2222222222222");
	    while(it.hasNext())
	    {
		String key = it.next();
		if(key.trim().length() > 0)
		    System.out.println(key);
	    }
	    */

	    /*
	    it = two.iterator();
	    System.out.println("\n2222222222222222222");
	    while(it.hasNext())
		System.out.println(it.next());
	    it = three.iterator();
	    System.out.println("\n333333333333333333333");
	    while(it.hasNext())
		System.out.println(it.next());
	    */
	}

}//////////
