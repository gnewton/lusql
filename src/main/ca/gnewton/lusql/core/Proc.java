package ca.gnewton.lusql.core;
import java.io.*;
import java.util.*;

/**
 * Describe class Proc here.
 *
 *
 * Created: Thu Feb 12 02:22:33 2009
 *
 * @author <a href="mailto:gnewton@">glen newton</a>
 * @version 1.0

   /proc/self/status:
   VmSize: The size of the virtual memory allocated to the process
   VmLck: The amount of locked memory
   VmRSS: The amount of memory mapped in RAM ( instead of swapped out )
   VmData: The size of the Data segment
   VmStk: The stack size
   VmExe: The size of the executable segment
   VmLib: The size of the library code
   VmPTE: Size of the Page Table entry

 */
public class Proc {
    static String meminfo = "/proc/meminfo";
    static String loadavg = "/proc/loadavg";
    static String status = "/proc/self/status";

    /**
     * Creates a new <code>Proc</code> instance.
     *
     */
    public Proc() {

    }

    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) {
	Map<String, String>map;
	
	while(true)
	{
	    map = Proc.mem();
	    System.out.println(map);
	    try
	    {
		Thread.sleep(1000);
		//Thread.sleep(1);
	    }
	    catch(Throwable t)
	    {
		t.printStackTrace();
	    }
	}

    }

    static boolean notLinux = false;
  
    static Map<String, String> mem()
	{
	    if(notLinux)
	    return null;

	  File m = new File(meminfo);
	  if(!m.exists() ||  !m.canRead())
	    {
	      notLinux = true;
	      return null;
	    }


	    Map<String, String>map = new HashMap<String, String>();
	    BufferedReader in = null;
	    try
	    {
		in = new BufferedReader(new InputStreamReader(new FileInputStream(meminfo)));
		String line = in.readLine();
		while(line != null)
		{
		    handleMeminfoLine(line, map);
		    line = in.readLine();
		}
		in.close();

		in = new BufferedReader(new InputStreamReader(new FileInputStream(status)));
		line = in.readLine();
		while(line != null)
		{
		    if(line.startsWith("Vm"))
			handleMeminfoLine(line, map);
		    line = in.readLine();
		}
		in.close();

		in = new BufferedReader(new InputStreamReader(new FileInputStream(loadavg)));
		map.put("loadavg", in.readLine().split(" ")[0]);

		map.put("vmFreeMemory", Long.toString(Runtime.getRuntime().freeMemory()));
			
	    }
	    catch(IOException e)
	    {
		e.printStackTrace();
	    }
	    finally
	    {
		if(in != null)
		    try
		    {
			in.close();
		    }
		    catch(Throwable t )
		    {
			t.printStackTrace();
		    }
	    }
	    return map;
	}

    static void handleMeminfoLine(String s, Map<String, String> map)
	{
	    s = s.replaceAll("kB", "");
	    String[]parts = s.trim().split(":");
	    map.put(parts[0].trim(), parts[1].trim());
	}

} //
