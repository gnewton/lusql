package ca.gnewton.lusql.driver.bdb;
import ca.gnewton.lusql.core.*;
import ca.gnewton.lusql.util.*;
import java.util.*;


/**
 * Describe class CheckRecordFilter here.
 *
 *
 * Created: Tue Aug 17 10:27:00 2010
 *
 * @author <a href="mailto:gnewton@chekov">glen</a>
 * @version 1.0
 */
public class CheckRecordFilter 
    extends BaseFilter
{
    public String description()
    {
	return "Verifies that all values in the DocSource are also in a BDB index";
    }
    
    public Properties explainProperties()
    {
	Properties p = new Properties();
	p.setProperty(BDBIndexPropKey, "Location of BDB index against which will be testing");
	p.setProperty(BDBCore.PrimaryKeyPropKey, "Name of field in DocSource Document to use as primary key for this BDB index.");
	return p;
    }
    
    /**
     * Describe indexName here.
     */
    private String indexName;

    /**
     * Describe sourceKey here.
     */
    private String sourceKey;

    /**
     * Describe sinkKey here.
     */
    private String sinkKey;
    public final static String BDBIndexPropKey = "bdbIndex";

    public final static String BDBSourcePKPropKey = "bdbSourceK";
    public final static String BDBFilterPKPropKey = "bdbFilterK";
    
    BDBCore core = null;
    public void init(MultiValueProp p) throws PluginException
    {
	if(p.containsKey(BDBIndexPropKey))
	    setIndexName(p.getProperty(BDBIndexPropKey).get(0));
	else
	    throw new PluginException("Missing index name: use property:" + BDBIndexPropKey);

	if(p.containsKey(BDBSourcePKPropKey))
	    setSourceKey(p.getProperty(BDBSourcePKPropKey).get(0));
	else
	    throw new PluginException("Missing Source Key: use property:" + BDBSourcePKPropKey);

	if(p.containsKey(BDBFilterPKPropKey))
	    setSinkKey(p.getProperty(BDBFilterPKPropKey).get(0));
	else
	    throw new PluginException("Missing Filter Key: use property:" + BDBFilterPKPropKey);

	core = new BDBCore(getIndexName(), 
			   false, 
			   true);
    }

    public Doc filter(Doc doc)
	throws ca.gnewton.lusql.core.FatalFilterException
    {

	String sourcePKValue = (doc.getFieldValues(getSourceKey())).get(0);
	// There is no corresponding record in the Filter BDB
	if(!core.containsKey(sourcePKValue))
	    throw new FatalFilterException("Source and filter BDB do not match: "
					   + "source key="
					   + getSourceKey()
					   + " value="
					   + sourcePKValue
					   + "  does not exist in filter BDB:"
					   + getIndexName()
					   );
	
	Doc filterRecord = core.getDoc(sourcePKValue);
	// The filter record does not give us its primary key
	if(!filterRecord.getProperties().containsKey(BDBCore.PrimaryKeyKey))
	    throw new FatalFilterException("Filter BDB does not have "
					   + BDBCore.PrimaryKeyKey
					   + " set in DocImp properties"
					   );

	

	return doc;
    }
    

    public void done()
	throws PluginException
	{
	    core.done();
	}

    /**
     * Get the <code>IndexName</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getIndexName() {
	return indexName;
    }

    /**
     * Set the <code>IndexName</code> value.
     *
     * @param newIndexName The new IndexName value.
     */
    public final void setIndexName(final String newIndexName) {
	this.indexName = newIndexName;
    }

    /**
     * Get the <code>SourceKey</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getSourceKey() {
	return sourceKey;
    }

    /**
     * Set the <code>SourceKey</code> value.
     *
     * @param newSourceKey The new SourceKey value.
     */
    public final void setSourceKey(final String newSourceKey) {
	this.sourceKey = newSourceKey;
    }

    /**
     * Get the <code>SinkKey</code> value.
     *
     * @return a <code>String</code> value
     */
    public final String getSinkKey() {
	return sinkKey;
    }

    /**
     * Set the <code>SinkKey</code> value.
     *
     * @param newSinkKey The new SinkKey value.
     */
    public final void setSinkKey(final String newSinkKey) {
	this.sinkKey = newSinkKey;
    }
}      

