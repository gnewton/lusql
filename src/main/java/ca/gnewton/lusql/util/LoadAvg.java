package ca.gnewton.lusql.util;
import java.lang.management.ManagementFactory;

import java.lang.management.OperatingSystemMXBean;

import java.util.Random;
import java.util.Random;


public class LoadAvg
{
	static Random random = new Random();
	
	static OperatingSystemMXBean osb = ManagementFactory.getOperatingSystemMXBean() ;
	
	public static final void main(String[] args)
	{
		System.out.println(osb.getSystemLoadAverage());
		checkAvg(3.0);
		
	}


	public static void checkAvg(double limit)
	{
		int count = 0;
		while(true){
			double loadAvg = osb.getSystemLoadAverage();
			//System.out.println("--- " + count + "  " + limit + " " + loadAvg);
			
			if(loadAvg < limit){
				//System.out.print("$");
				break;
			}
			else
				{
					System.err.print("*");			
					try{
						Thread.currentThread().sleep((int)(300+ count*loadAvg*loadAvg*50));
					}
					catch(Exception e){
						e.printStackTrace();
					}
				}

			++count;

			if(random.nextInt(1000) < (count*count)/(loadAvg*loadAvg)){
				System.err.println("+");
				break;
			}

		}
		

	}
	

}
