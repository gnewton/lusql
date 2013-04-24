package ca.gnewton.lusql.util;
import java.util.*;

/**
 * Describe class MultiValueProp here.
 *
 *
 * Created: Thu Aug  5 01:34:29 2010
 *
 * @author <a href="mailto:gnewton@chekov">glen</a>
 * @version 1.0
 */
public class MultiValueProp {
	Map<String, List<String>> map = null;
	Set<String> uniques = null;
	/**
	 * Creates a new <code>MultiValueProp</code> instance.
	 *
	 */
	public MultiValueProp() 
	{
		map = new HashMap<String, List<String>>();
	}


	public void setUnique(String k)
	{
		if(uniques == null)
			uniques = new HashSet<String>();
		uniques.add(k);
	}

	public Set<String>keySet()
	{
		return map.keySet();
	}
	
    
    
	MultiValueProp copy(Map<String, List<String>>m)
	{
		MultiValueProp newm = new MultiValueProp();
		if(map != null)
			{
		
				Iterator<String>it = map.keySet().iterator();
				while(it.hasNext())
					{
						String key = it.next();
						List<String> values = map.get(key);
						for(String v: values)
							newm.add(key, v);
					}	
			}
	
		if(uniques != null)
			{
				Iterator<String>it = uniques.iterator();
				while(it.hasNext())
					newm.setUnique(it.next());
			}
	
		return newm;
	}
    

	public MultiValueProp copy()
	{
		return copy(map);
	}
    

	public List<String>get(String key)
	{
		return getValues(key);
	}
	
	public boolean containsKey(String k)
	{
		return map.containsKey(k);
	}

	public List<String>getProperty(String k)
	{
		return getValues(k);
	}
    

	public List<String>getValues(String key)
	{
		if(!map.containsKey(key))
			return null;
		else
			return map.get(key);
	}

	public void put(String k, String v)
	{
		add(k,v);
	}

	public void put(String k, List<String> v)
	{
		map.put(k,v);
	}

	public void remove(String k)
	{
		map.remove(k);
	}
    
	public void setProperty(String k, String v)
	{
		add(k,v);
	}

	public final String MultiSeparator="|_|";
    
	//Assumes that multiple values are in Properties value, separated by |_|;
	// name=fred|_|bill|_|harr smith|_|
	public void loadFromProperties(Properties p)
	{
		Iterator<String> it = p.stringPropertyNames().iterator();

		while(it.hasNext())
			{
				String k = it.next();
				String v = p.getProperty(k);
				if(v.indexOf(MultiSeparator) ==-1)
					put(k, v);
				else
					put(k, v.split(MultiSeparator));
			}
	}
    
	public void put(String k, String[] v)
	{
		for(String part: v)
			put(k,part.trim());
	}
    

	public void add(String k, String v)
	{
		if(uniques != null
		   && uniques.contains(k))
			remove(k);
		List<String>list = null;
	
		if(!map.containsKey(k))
			{
				list = new ArrayList<String>();
				map.put(k, list);
			}
		else
			list = map.get(k);
		list.add(v);
	}
    
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		Iterator<String>it = map.keySet().iterator();
		while(it.hasNext())
			{
				String key = it.next();
				List<String> values = map.get(key);
				sb.append("\n " + key + ":");
				for(String v:values)
					sb.append("[" + v + "]");
			}
	
		return sb.toString();
	
	}
    

}//
