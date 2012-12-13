/**
 * Describe class F here.
 *
 *
 * Created: Wed Aug 18 05:20:55 2010
 *
 * @author <a href="mailto:gnewton@chekov">glen</a>
 * @version 1.0
 */
import java.util.*;


public class F {

    /**
     * Creates a new <code>F</code> instance.
     *
     */
    public F() {

    }

    /**
     * Describe <code>main</code> method here.
     *
     * @param args a <code>String</code> value
     */
    public static final void main(final String[] args) 
    {
	int numTerms = 5000;
	//int numTerms = 50;
	int minDocSize = 5000;
	//int minDocSize = 100;
	int maxDocSize=2000;
	//int numDocs = 1000;
	int numDocs = 10000;
	

	int dist[] = new int[numTerms];
	for(int i=0; i<numTerms; i++)
	    dist[i] = 0;
	
	StringBuffer sb = new StringBuffer(maxDocSize);
	Random r = new Random();

	int count = 0;
	
	for(int j=0; j<numDocs; j++)
		//while(true)
	    {
		for(int i=0; i<minDocSize; i++)
		    {
			int num = 0;
			while(true)
			    {
				num=r.nextInt(numTerms);
				int tmp = r.nextInt(num+1);
				//System.out.println(i + ":" + num + ":" + tmp);
				if(tmp < 2)
				    break;
			    }
			//System.out.println(i + ": " + num);
			dist[num] = dist[num] +1;
			++count;
		    }
	    }

	for(int i=0; i<numTerms; i++)
	    System.out.println(i 
				+ ":"
				+ ((double)dist[i]/(double)count));
	
    }

}
