package ca.gnewton.lusql.example;
 
import ca.gnewton.lusql.core.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.apache.log4j.*;
import org.apache.lucene.document.*;
import ca.gnewton.lusql.util.*;

/**
 * Describe class FileFullTextFilter here.
 *
 *
 * Created: Wed Jan 16 03:47:09 2008
 *
 * @author <a href="mailto:glen.newton@nrc-cnrc.gc.ca">Glen Newton</a> CISTI Research 
 * @copyright CISTI / National Research Council Canada
 * @version 0.9
 * License: Apache v2 http://www.apache.org/licenses/LICENSE-2.0.txt
 * 
 */

public class FileFullTextFilter 
    extends BaseFilter
{
	//static Category cat = Category.getInstance(LuSql.class.getName());

    public String description()
	{
	    return "Filter that uses a field in the source document to find a file on the filesystem and adds this the contents of this file as a new field to the document to be indexed";
	} 
    
    boolean doNotReadFile = false;

    // Property keys
    public final static String DoNotReadFileFieldKey = "doNotReadFileField";
    public final static String GroupFieldKey = "groupField";
    public final static String LuceneIndexParametersKey = "luceneIndexParameters";
    public final static String MaxTextNumCharsKey = "maxTextChars";
    public final static String MaxTextProportionKey = "maxTextProportion";
    public final static String MinTextNumCharsKey = "minTextChars";
    public final static String MinTextProportionKey = "minTextProportion";
    public final static String OffsetTextCharsKey = "offset";
    public final static String SinkFieldKey = "sinkField";
    public final static String SourceFieldKey = "sourceField";
    public final static String StripFieldKey = "stripField";
    public final static String TextBaseDirKey = "fullTextDir";
    public final static String WeightKey = "weight";
    ////

    public Properties explainProperties()
	{
	    Properties p = new Properties();
	    p.setProperty(TextBaseDirKey, "Base directory for text files.  Default:" + BaseDir);
	    p.setProperty(LuceneIndexParametersKey, "Lucene field index parameters. See \"-i\" LuSql options");
	    p.setProperty(MaxTextProportionKey, "The location in the file as a proportion before which to use the characters. So '45' will take from 0 to 45% of the text. "
			  + MinTextProportionKey 
			  + " is also used, the portion of the document defined by these 2 ranges is used, i.e. 10,45 starts 10% in and goes to 45%. "
			  +" Note this modifier is applied before all other text modifiers except"
			  + MinTextNumCharsKey
			  + ". Default:" 
			  + maxTextProportion);
	    p.setProperty(MinTextProportionKey, "The location in the file as a proportion after which to use the characters."
			  + ".  Default:" 
			  + minTextProportion);

	    p.setProperty(MaxTextNumCharsKey, "The # of characters from the beginning of the text file to use. int >=0. Note that this is unchanged when using " 
			  + OffsetTextCharsKey
			  + ".  Default:" 
			  + offsetTextChars);
	    
	    p.setProperty(OffsetTextCharsKey, "Do not use the first N characters. int>=0.  Default:" + offsetTextChars);

	    p.setProperty(MinTextNumCharsKey, "If the # of characters is less than this, discard and do not index. int >=0.  Default:" + minTextNumChars);

	    p.setProperty(SourceFieldKey, 
			  "The field in the index that contains the partial path information to the text file to be indexed.  Default: " 
			  + sourceField);

	    p.setProperty(SinkFieldKey, "The field in to which the indexed text is to go.  Default: " + sinkField);
	    p.setProperty(GroupFieldKey, "Group field to count missing fields by");
	    p.setProperty(StripFieldKey, "Field to be removed from full text (acts like a stop word)");
	    p.setProperty(DoNotReadFileFieldKey, "If true, file is not read, but its existance IS checked");

	    return p;
	}

    Map<String, Integer>groupFieldCount = new HashMap<String, Integer>();

    /**
     * Describe textNumChars here.
     */
    private int maxTextNumChars = Integer.MAX_VALUE;

    /**
     * Describe minTextNumChars here.
     */
    private int minTextNumChars = 0;

    /**
     * Describe sourceField here.
     */
    private String sourceField = "txtUrl";

    /**
     * Describe sinkText here.
     */
    private String sinkField = "contents";

    /**
     * Describe offsetTextChars here.
     */
    private int offsetTextChars = 0;

    /**
     * Describe minTextProportion here.
     */
    private int minTextProportion=0;

    /**
     * Describe maxTextProportion here.
     */
    private int maxTextProportion=100;

    /**
     * Describe weight here.
     */
    private float weight = 1.0f;

    /**
     * Describe groupField here.
     */
    private String groupField = null;

    /**
     * Describe stripField here.
     */
    private String stripField = null;



    LuceneFieldParameters textParas = new LuceneFieldParameters(
								//Field.Index.TOKENIZED, 
								Field.Index.NO, 
								//Field.Store.NO, 
								Field.Store.YES, 
								//Field.TermVector.WITH_POSITIONS_OFFSETS
								Field.TermVector.NO 
								);


    // The base directory for all files
    String BaseDir = "/mnt/data/dartimin/dartejos/";
    final static int GZIPFACTOR=4;
    public Doc filter(Doc doc)
	throws ca.gnewton.lusql.core.FatalFilterException
	{
	    if(doc == null)
		{
			//cat.debug("Doc is null");
		    return null;
		}
	    
	    //System.out.println(doc);
	    //The text field (which is the 'rawUrl' field in the db) is the path fragment for the 
	    // fulltext file, which is compressed with gzip;
	    List<String> fileFields = doc.getFieldValues(sourceField);
	    if(fileFields == null)
	    {
		    //cat.error("Problem: no \""
		    //				   + sourceField
		    //				   + "\" field in document");
		getLuSql().setFatalError(true);
		return doc;
	    }
	    String fileField = fileFields.get(0);
	    if(fileField == null)
	    {
		    //cat.error("No filename field in article id=" +  doc.getFieldValues("id").get(0));
		getLuSql().setFatalError(true);
		//throw new Fatal
		//return null;
		return doc;
	    }
	    String issn = null;
	    if(groupField != null
	       && doc.getFieldValues(groupField) != null)
	    {
		issn= doc.getFieldValues(groupField).get(0);
	    }


	    String text = readFileToString(BaseDir + fileField, 65536, issn);


	    if(text != null)
	    {
		String fText = filterText(text);
		text = strip(doc, fText);
		doc.addField(sinkField, fText, textParas, weight);
	    }
	    
	    addXMLField(doc, fileField);

	    return doc;
	}

    synchronized void incrementCount(String countField)
	{
	    Integer count = null;
	    if(groupFieldCount.containsKey(countField))
		count = groupFieldCount.get(countField);
	    else
		count = new Integer(0);
	    ++count; 
	    groupFieldCount.put(countField, count);
	}

    @Override
    public void init(MultiValueProp p)
	throws ca.gnewton.lusql.core.PluginException
	{
	    extractProperties(p);
	}

  /**
   * Get the <code>Properties</code> value.
   *
   * @return a <code>Properties</code> value
   */
    public final MultiValueProp getProperties() {
	return null;
  }


    String filterText(String s)
	{
	    //if(s == null)
	    //return null;
	    return s.replaceAll("&#10;", " ");
	    //return s;
	}
    
    Reader makeReader(File f, int bufSize, int estSize)
	throws FileNotFoundException, IOException
	{
	    try
		{
		    BufferedReader reader = null;
		    if(f.getName().endsWith(".gz"))
			{
			    // Compressing zero length files results in a gzip file
			    // of 10 bytes. Java GZIP chokes on this
			    // java.io.EOFException: Unexpected end of ZLIB input stream
			    //
			    if(f.length() < 11)
				return null;
			    reader = new BufferedReader(
							new InputStreamReader(
									      new GZIPInputStream(
												  new FileInputStream(f))), 
							estSize);
			}
		    else
			{
			    reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)), estSize);
			}
		    return reader;
		}
	    catch(FileNotFoundException t)
		{
		    System.out.println("FileNotFound file=" + f.getName());
		    t.printStackTrace();
		}
	    catch(IOException io)
		{
		    System.out.println("IOException file=" + f.getName());
		    io.printStackTrace();
		}
	    return null;
	}

    void addXMLField(Doc doc, String fileField)
    {
	if(fileField.contains(".txt"))
	    {
		String xml = fileField.replace(".txt",".xml");
		//System.out.println(fileField + ": " + xml);
		File x = new File(BaseDir + xml + ".gz");
		if(x.exists())
		    {
			//System.out.println("  ----> exists");
			try
			    {
				String text = readFileToString(x.getAbsolutePath(), 65536, (String)null);
				doc.addField("xml", text, textParas, weight);
			    }
			catch(Throwable t)
			    {
				t.printStackTrace();
			    }
		    }
	    }
    }

    String readFileToString(String fileName, int bufSize, String issn)
	throws ca.gnewton.lusql.core.FatalFilterException
	{
	    Reader reader = null;
	     StringBuilder sb = null;
	     int estSize=40000;
	    try
	    {
		if(fileName == null)
		    return null;
		File f=null;
		f = new File(fileName);
		if(! (f.exists() && f.canRead()))
		{
		    f = new File(fileName+ ".gz");
		    if(! (f.exists() && f.canRead()))
			{
				if(LuSql.verbose){
				    //cat.info("FileFullTextFilter:: Unable to read file: " + fileName);
				}
				
			    if(issn != null)
			    {
				incrementCount(issn);
			    }
			    return null; // bad!
			}
		}

		// This is here and not above previous as I want to know if the file is missing, even if
		// I don't want to read it
		if(doNotReadFile)
		    return null;

		// Estimate the ungzipped size for good buffer size estimate
		if(fileName.endsWith(".gz"))
		    estSize = (int)(f.length() * GZIPFACTOR);
		else
		    estSize = (int)f.length();

		reader = makeReader(f, bufSize, estSize);
		// 
		if(reader == null)
		    {
			    //cat.debug("Reader is null for file: " + fileName);
			return "";
		    }

		estSize = 4096;
		bufSize = estSize;
		if(bufSize <0)
		    bufSize = estSize;
		sb = new StringBuilder(estSize);
		char[] cbuf = new char[bufSize];
		int n = 0;

		while((n = reader.read(cbuf, 0, bufSize))>0 && sb.length() < maxTextNumChars)
		    {
			sb.append(cbuf,0,n);
			sb.append(" ");
		    }
	    }
	    catch(Throwable t)
	    {
		    //cat.error("Exception: Problem with file: " + fileName + " EstSize=" + estSize);
		t.printStackTrace();
		//throw new ca.gnewton.lusql.core.FatalFilterException();
		if(sb == null)
		    return "";
		return sb.toString();
	    }
	    finally
	    {
		try
		{
		    if(reader!=null)
			reader.close();
		}
		catch(Throwable t)
		{
		    // OK
		}
		
	    }
	    try
	    {
		// only on of these should be applied... FIXX??
		//if(sb.length() < minTextNumChars)
		//return null;
		String s = sb.toString();

		if(s.length() > maxTextNumChars)
		    {
			s = s.substring(0, maxTextNumChars);
		    }
		if(maxTextProportion < 100 || minTextProportion > 0)
		    s = s.substring((int)((float)s.length() * (minTextProportion/100.0)), 
				    (int)((float)s.length() * (maxTextProportion/100.0)));

		if(maxTextNumChars != Integer.MAX_VALUE
		    && maxTextNumChars < s.length())
		    s = s.substring(0, maxTextNumChars);
		return s;
	    }
	    catch(Throwable t)
	    {
		 t.printStackTrace();
		 throw new NullPointerException();
	    }

	}

    void extractProperties(MultiValueProp p)
	{
	    if(p == null)
		return;
	    if(p.containsKey(TextBaseDirKey))
		{
		    BaseDir = p.getProperty(TextBaseDirKey).get(0);
		    if(BaseDir.charAt(BaseDir.length()-1) != File.separatorChar)
			BaseDir = BaseDir + File.separator; 
		}
	    if(p.containsKey(LuceneIndexParametersKey))
		textParas = new LuceneFieldParameters(p.getProperty(LuceneIndexParametersKey).get(0));
	    if(p.containsKey(MinTextProportionKey))
		setMinTextProportion(Integer.parseInt(p.getProperty(MinTextProportionKey).get(0)));
	    if(p.containsKey(MaxTextProportionKey))
		setMaxTextProportion(Integer.parseInt(p.getProperty(MaxTextProportionKey).get(0)));
	    if(p.containsKey(MaxTextNumCharsKey))
		setMaxTextNumChars(Integer.parseInt(p.getProperty(MaxTextNumCharsKey).get(0)));
	    if(p.containsKey(MinTextNumCharsKey))
		setMinTextNumChars(Integer.parseInt(p.getProperty(MinTextNumCharsKey).get(0)));
	    if(p.containsKey(SourceFieldKey))
		setSourceField(p.getProperty(SourceFieldKey).get(0));
	    if(p.containsKey(SinkFieldKey))
		setSinkField(p.getProperty(SinkFieldKey).get(0));
	    if(p.containsKey(GroupFieldKey))
		groupField = p.getProperty(GroupFieldKey).get(0);
	    if(p.containsKey(StripFieldKey))
		stripField = p.getProperty(StripFieldKey).get(0);

	    if(p.containsKey(DoNotReadFileFieldKey))
		doNotReadFile = Boolean.valueOf(p.getProperty(DoNotReadFileFieldKey).get(0)).booleanValue();
	}


    /**
     * Get the <code>MaxTextNumChars</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getMaxTextNumChars() {
	return maxTextNumChars;
    }

    /**
     * Set the <code>MaxTextNumChars</code> value.
     *
     * @param newMaxTextNumChars The new MaxTextNumChars value.
     */
    public final void setMaxTextNumChars(final int newMaxTextNumChars) {
	this.maxTextNumChars = newMaxTextNumChars;
    }

    /**
     * Get the <code>MinTextNumChars</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getMinTextNumChars() {
	return minTextNumChars;
    }

    /**
     * Set the <code>MinTextNumChars</code> value.
     *
     * @param newMinTextNumChars The new MinTextNumChars value.
     */
    public final void setMinTextNumChars(final int newMinTextNumChars) {
	this.minTextNumChars = newMinTextNumChars;
    }

    /**
     * Get the <code>SourceField</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getSourceField() {
	return sourceField;
    }

    /**
     * Set the <code>SourceField</code> value.
     *
     * @param newSourceField The new SourceField value.
     */
    public final void setSourceField(final String newSourceField) {
	this.sourceField = newSourceField;
    }

    /**
     * Get the <code>SinkField</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getSinkField() {
	return sinkField;
    }

    /**
     * Set the <code>SinkField</code> value.
     *
     * @param newSinkField The new SinkField value.
     */
    public final void setSinkField(final String newSinkField) {
	this.sinkField = newSinkField;
    }

    /**
     * Get the <code>OffsetTextChars</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getOffsetTextChars() {
	return offsetTextChars;
    }

    /**
     * Set the <code>OffsetTextChars</code> value.
     *
     * @param newOffsetTextChars The new OffsetTextChars value.
     */
    public final void setOffsetTextChars(final int newOffsetTextChars) {
	this.offsetTextChars = newOffsetTextChars;
    }

    /**
     * Get the <code>MinTextProportion</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getMinTextProportion() {
	return minTextProportion;
    }

    /**
     * Set the <code>MinTextProportion</code> value.
     *
     * @param newMinTextProportion The new MinTextProportion value.
     */
    public final void setMinTextProportion(final int newMinTextProportion) {
	this.minTextProportion = newMinTextProportion;
    }

    /**
     * Get the <code>MaxTextProportion</code> value.
     *
     * @return an <code>int</code> value
     */
    public final int getMaxTextProportion() {
	return maxTextProportion;
    }

    /**
     * Set the <code>MaxTextProportion</code> value.
     *
     * @param newMaxTextProportion The new MaxTextProportion value.
     */
    public final void setMaxTextProportion(final int newMaxTextProportion) {
	this.maxTextProportion = newMaxTextProportion;
    }

    /**
     * Get the <code>Weight</code> value.
     *
     * @return a <code>float</code> value
     */
    public final float getWeight() {
	return weight;
    }

    /**
     * Set the <code>Weight</code> value.
     *
     * @param newWeight The new Weight value.
     */
    public final void setWeight(final float newWeight) {
	this.weight = newWeight;
    }

    /**
     * Get the <code>GroupField</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getGroupField() {
	return groupField;
    }

    /**
     * Set the <code>GroupField</code> value.
     *
     * @param newGroupField The new GroupField value.
     */
    public final void setGroupField(final String newGroupField) {
	this.groupField = newGroupField;
    }

    @Override
    public void done()
	{
	    try
	    {
		Writer output = new BufferedWriter(new FileWriter("countField" 
								  + LuSqlFields.LuSqlInfoSuffix));
		Iterator<String> it = groupFieldCount.keySet().iterator();
		while(it.hasNext())
		{
		    String key = it.next();
		    output.write(key + " " + groupFieldCount.get(key) + "\n");
		    //if(LuSql.verbose)
			    //cat.info(key + " " + groupFieldCount.get(key));
		}
	    output.close();
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace(); 
	    }
	}

    /**
     * Get the <code>StripField</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getStripField() {
	return stripField;
    }

    /**
     * Set the <code>StripField</code> value.
     *
     * @param newStripField The new StripField value.
     */
    public final void setStripField(final String newStripField) {
	this.stripField = newStripField;
    }

    //Pattern p = Pattern.compile("\\w\\s");

    synchronized String strip(Doc doc, String s)
	{ 

	    if(doc.getFieldValues(stripField) == null)
		return s;
	    String strip = doc.getFieldValues(stripField).get(0).toLowerCase();

	    s = s.toLowerCase();
	    s = s.replace("\u240A", "");
	    s = s.replace("\u240D", " ");
	    int n = s.length();
	    s = s.replace(strip, " ");
	    /* Removed 2009 01 18 GN
	    if(n != s.length())
	    {
		System.out.println("\t---> " + stripField + "="+ doc.getFieldValues(stripField));
		System.out.println("\t" + doc.getFieldValues("txtUrl").get(0) + ":" + (s.length()-n));
	    }
	    */
	    for(int i=1; i<40; i++)
		s = s.replace("[" + i + "]", " ");

	    /* Removed 2009 01 18 GN
	    n = s.length();
	    if(n != s.length())
	    {
		System.out.println("\t\t---> " + stripField + "="+ doc.getFieldValues(stripField));
		System.out.println("\t\t" + doc.getFieldValues("txtUrl").get(0) + ":" + (s.length()-n));
	    }
	    */

	    return s;
	}

    public String showState(int n)
    {
	StringBuilder sb = new StringBuilder();
	sb.append(ca.gnewton.lusql.util.Util.offset("Filter: LongDocumentDocSource",n));
	sb.append(ca.gnewton.lusql.util.Util.offset("maxTextNumChars:" + maxTextNumChars, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("minTextNumChars:" + minTextNumChars, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("sinkField:" + sinkField, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("minTextProportion:" + minTextProportion, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("maxTextProportion:" + maxTextProportion, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("weight:" + weight, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("groupField:" + groupField, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("stripField:" + stripField, n+1));
	sb.append(ca.gnewton.lusql.util.Util.offset("BaseDir:" + BaseDir, n+1));

	return sb.toString();
    }
}//////////
