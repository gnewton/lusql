package ca.gnewton.lusql.core;
import org.apache.lucene.document.*;
import java.util.*;
import ca.gnewton.lusql.util.*;




/**
 * Describe class FieldMapFilter here.
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
 *
 */
public class FieldMapFilter 
    extends BaseFilter
{
    public String showState(int n)
    {
	String s = new String();
	s += ca.gnewton.lusql.util.Util.offset("FieldMapFilter: ",n);
	
	Iterator<String>it = fieldMap.keySet().iterator();
	while(it.hasNext())
	    {
		String k = it.next();
		s += ca.gnewton.lusql.util.Util.offset(k + ":" + fieldMap.get(k), n+1);

	    }
	

	return
	    "\tFieldMapFilter\n"
	    + "\t onlyMap=" + onlyMap
	    + s;
    }


    public String description()
	{
	    return "Filter that maps fields that it is supplied in the document into new fields (in the same document). Note that the old fields are removed from the document";
	}

    /**
     * Describe luSql here.
     */
    private LuSql luSql;

  /**
   * Describe properties here.
   */
  private MultiValueProp properties;

    /**
     * Describe onlyMap here.
     */
    private boolean onlyMap = false;

    /**
     * Describe fieldMap here.
     */
    
    private Map<String,String> fieldMap = null;
  public Doc filter(Doc doc)
      throws ca.gnewton.lusql.core.FatalFilterException
  {
      if(fieldMap != null)
	  mapFields(doc);
      return doc;
  }

    @Override
    public void init(MultiValueProp p)
	throws ca.gnewton.lusql.core.PluginException
	{
	    setProperties(p);
	}

    void mapFields(Doc doc)
	{
	    Iterator<String>it = fieldMap.keySet().iterator();
	    while(it.hasNext())
	    {
		String oldField = it.next();
		String newField = fieldMap.get(oldField);
		if(newField == null)
		    continue;
		List<String> values = doc.getFieldValues(oldField);
		if(values == null)
		    continue;
		for(String value: doc.getFieldValues(oldField))
		    doc.addField(newField, value);
		doc.addFieldParameter(newField, doc.getFieldParameters(oldField));
		doc.removeField(oldField);
	    }
	    if(onlyMap == true)
	    {
		List<String> badFields = new ArrayList<String>();
		it = doc.getFieldNames();
		while(it.hasNext())
		{
		    String f = it.next();
		    if(!fieldMap.containsValue(f))
			badFields.add(f);
		}
		for(String f: badFields)
		    doc.removeField(f);
	    }
	}
  /**
   * Get the <code>Properties</code> value.
   *
   * @return a <code>Properties</code> value
   */
  public final MultiValueProp getProperties() {
    return properties;
  }




    /**
     * Get the <code>FieldMap</code> value.
     *
     * @return a <code>Map<String,String></code> value
     */
    public final Map<String,String> getFieldMap() {
	return fieldMap;
    }

    /**
     * Set the <code>FieldMap</code> value.
     *
     * @param newFieldMap The new FieldMap value.
     */
    public final void setFieldMap(final Map<String, String> newFieldMap) {
	this.fieldMap = newFieldMap;
    }

    /**
     * Get the <code>OnlyMap</code> value.
     *
     * @return a <code>boolean</code> value
     */
    public final boolean isOnlyMap() {
	return onlyMap;
    }

    /**
     * Set the <code>OnlyMap</code> value.
     *
     * @param newOnlyMap The new OnlyMap value.
     */
    public final void setOnlyMap(final boolean newOnlyMap) {
	this.onlyMap = newOnlyMap;
    }
}
