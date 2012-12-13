package ca.gnewton.lusql.core;

/**
 * Describe class SubQuery here.
 *
 *
 * Created: Fri Oct 24 15:24:23 2008
 *
 * @author <a href="mailto:gnewton@">Glen Newton</a>
 * @version 1.0
 */
public class SubQuery 
{
    /**
     * Describe keyMeta here.
     */
    static public String KeyMeta = "@";
    static String Separator ="\\|";
    static String NSeparator =" ";

    /**
     * Describe fieldParameters here.
     */
    private String fieldParameters;

    /**
     * Describe docField here.
     */
    private String docField;

    /**
     * Describe query here.
     */
    private String query;

    /**
     * Describe cached here.
     */
    private boolean cached = false;



    /**
     * Creates a new <code>SubQuery</code> instance.
     *
     */
    public SubQuery(String s, String globalP) 
	{
	    System.out.println("SubQuery: " + s + "  :: " + globalP);
	    try
	    {
		// This subquery should be cached...
		if(s.charAt(0) == '*')
		{
		    setCached(true);
		    s = s.substring(1);
		}
		String[] parts = s.split(Separator);
		setDocField(parts[0]);
		if(parts.length == 3)
		{
		    setFieldParameters(parts[1]);
		    setQuery(parts[2]);
		}
		else
		{
		    setFieldParameters(globalP);
		    setQuery(parts[1]);
		}
	    }
	    catch(ArrayIndexOutOfBoundsException a)
	    {
		a.printStackTrace();
		System.err.println("SubQuery string=" + s);
		throw a;
	    }
	}

    // this should be "NNN" or "NNN NNN ...NNN"
    void extractLuceneFieldParameters(String lfp)
	{
	    String[] parts = lfp.split(NSeparator);
	    // NNN
	    if(parts == null)
	    {
		
	    }
	    else // NNN NNN NNN
	    {
		
	    }
	}

    /**
     * Get the <code>FieldParameters</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getFieldParameters() {
	return fieldParameters;
    }

    /**
     * Set the <code>FieldParameters</code> value.
     *
     * @param newFieldParameters The new FieldParameters value.
     */
    public final void setFieldParameters(final String newFieldParameters) {
	this.fieldParameters = newFieldParameters;
    }

    /**
     * Get the <code>DocField</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getDocField() {
	return docField;
    }

    /**
     * Set the <code>DocField</code> value.
     *
     * @param newDocField The new DocField value.
     */
    public final void setDocField(final String newDocField) {
	this.docField = newDocField;
    }

    /**
     * Get the <code>Query</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getQuery() {
	return query;
    }

    /**
     * Set the <code>Query</code> value.
     *
     * @param newQuery The new Query value.
     */
    public final void setQuery(final String newQuery) {
	this.query = newQuery;
    }

    String makeQuery(String keyValue)
	{
	    return getQuery().replace(KeyMeta, keyValue);
	}

    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) 
	{
	    String s = "id|222|select * from Author where id=$1";

	    SubQuery sq = new SubQuery(s, null);
	    System.out.println(s);
	    System.out.println(sq.makeQuery("787"));
	}

    /**
     * Get the <code>KeyMeta</code> value.
     *
     * @return a <code>String</code> value
     */
    static public final String getKeyMeta() {
	return KeyMeta;
    }

    /**
     * Set the <code>KeyMeta</code> value.
     *
     * @param newKeyMeta The new KeyMeta value.
     */
    static public final void setKeyMeta(final String newKeyMeta) {
	KeyMeta = newKeyMeta;
    }

    /**
     * Get the <code>Cached</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isCached() {
	return cached;
    }

    /**
     * Set the <code>Cached</code> value.
     *
     * @param newCached The new Cached value.
     */
    public final void setCached(final boolean newCached) {
	this.cached = newCached;
    }
}
