
package ca.gnewton.lusql.driver.json;

import ca.gnewton.lusql.core.Doc;
import ca.gnewton.lusql.core.DocImp;
import org.json.simple.*;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;


public class Doc2Json
{
	

	public static final String doc2json(Doc d)
	{
		JSONObject j=new JSONObject();

		//Map<String, List<String>> d.getFields();
		Iterator<String>fieldNames = d.getFieldNames();
		while(fieldNames.hasNext()){
			String fieldName = fieldNames.next();

			JSONArray values = new JSONArray();

			List<String> fieldValues = d.getFieldValues(fieldName);
			for(String v:fieldValues){
				values.add(v);
			}
			j.put(fieldName, values);
		}
		return j.toString();
	}

	public static final Doc json2doc(JSONObject j)
	{
		Doc d = new DocImp();

		Iterator<String>keys=j.keySet().iterator();
		while(keys.hasNext()){
			String key = keys.next();
			JSONArray jsonValues = (JSONArray)j.get(key);
			List<String>values = new ArrayList<String>(jsonValues.size());
			for(int i=0; i<jsonValues.size(); i++){
				values.add(jsonValues.get(i).toString());
			}
			d.addFields(key, values, null);
		}
		return d;
	}
	
}

