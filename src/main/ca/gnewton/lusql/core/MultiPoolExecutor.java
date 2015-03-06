
package ca.gnewton.lusql.core;

import java.util.concurrent.*;
import java.util.*;
 import java.util.concurrent.locks.ReentrantLock;

class MultiPoolExecutor
    implements ExecutorService
{
    List<ThreadPoolExecutor> tpe = null;

    //      Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first. 

    public MultiPoolExecutor()
    {
	System.err.println("Constructor start");
	init();
	System.err.println("Constructor end");
    }

    private final ReentrantLock lock = new ReentrantLock();
    ThreadPoolExecutor getBest()
    {
	ThreadPoolExecutor best = tpe.get(0);
	lock.lock();  // block until condition holds
	try {
	    for(int i=1; i<tpe.size(); i++)
		if(tpe.get(i).getQueue().size() < best.getQueue().size())
		    best = tpe.get(i);
	} 
	finally 
	    {
		lock.unlock();
	    }
	return best;
    }

    void init()
    {
	int numThreads = 64;
	int n = Runtime.getRuntime().availableProcessors();
	//n = 1;
	tpe = new ArrayList<ThreadPoolExecutor>(n) ;
	System.err.println("tpe size=" + n);
	System.err.println("#threads=" + numThreads);
	System.err.println("qsize=" + numThreads);
	System.err.println("#threads per ThreadPoolExecutor=" + numThreads/n);

	for(int i=0; i<n; i++)
	    {
		System.err.println("tpe size=" + i);
		BlockingQueue bq = new ArrayBlockingQueue<Runnable>(numThreads);
		
		tpe.add(new ThreadPoolExecutor(1, // minThreads
					       //numThreads / n, //maxThreads
					       numThreads, //maxThreads
					     16l, // timeout
					     TimeUnit.SECONDS,
					     bq,
					     new ThreadPoolExecutor.CallerRunsPolicy())
			);
	    }
    }

    // Executes the given command at some time in the future.    
    public void execute(Runnable command)
    {
	getBest().execute(command);
    }

    public boolean awaitTermination(long timeout, TimeUnit unit)
	throws InterruptedException
    {
	boolean ret = true;
	for(ThreadPoolExecutor t: tpe)
	    {
		ret = ret && t.awaitTermination(timeout, unit);
	    }
	return ret;
    }
    


    // Executes the given tasks, returning a list of Futures holding their status and results when all complete.
    public <T> List<Future<T>> invokeAll(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks)
	throws InterruptedException
    {
	return getBest().invokeAll(tasks);
    }
    
    // Executes the given tasks, returning a list of Futures holding their status and results when all 
    //  complete or the timeout expires, whichever happens first.
    public <T> List<Future<T>> invokeAll(
					 java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks,
					 long timeout,
					 java.util.concurrent.TimeUnit unit)
	throws InterruptedException
    {
	return getBest().invokeAll(tasks, timeout, unit);
    }

    // Executes the given tasks, returning the result of one that 
    // has completed successfully (i.e., without throwing an exception), if any do.
    public <T> T invokeAny(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks)
	throws InterruptedException, ExecutionException
    {
	return getBest().invokeAny(tasks);
    }
    

	//Executes the given tasks, returning the result of one that has completed successfully 
	// (i.e., without throwing an exception), if any do before the given timeout elapses.
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) 
	throws InterruptedException, ExecutionException, TimeoutException
    {
	return getBest().invokeAny(tasks, timeout, unit);
    }


	// Returns true if this executor has been shut down.
	public boolean 	isShutdown()
	{
	    boolean shut = false;
	    for(ThreadPoolExecutor t: tpe)
		{
		    shut = shut && t.isShutdown();
		}
	    return shut;
	}

	// Returns true if all tasks have completed following shut down.
	public boolean isTerminated()
	{
	    boolean term = false;
	    for(ThreadPoolExecutor t: tpe)
		{
		    term = term && isTerminated();
		}
	    return term;
	}

	// Initiates an orderly shutdown in which previously submitted tasks are 
	// executed, but no new tasks will be accepted.
	public void shutdown()
	{
	    for(ThreadPoolExecutor t: tpe)
		{
		    t.shutdown();
		}
	}
    
	// Attempts to stop all actively executing tasks, halts the processing 
	// of waiting tasks, and returns a list of the tasks that were awaiting execution. 
	public List<Runnable> shutdownNow()
	{
	    List<Runnable> tasks = new ArrayList<Runnable>();
	    for(ThreadPoolExecutor t: tpe)
		{
		    tasks.addAll(t.shutdownNow());
		}
	    return tasks;
	}

	// Submits a value-returning task for execution and returns a Future representing the pending results of the task.    
	public <T> Future<T> submit(Callable<T> task)
	{
	    return getBest().submit(task);
	}

	// Submits a Runnable task for execution and returns a Future representing that task.
	public Future<?> submit(Runnable task)
	{
	    return getBest().submit(task);
	}
    
	// Submits a Runnable task for execution and returns a Future representing 
	// that task that will upon completion return the given result
	public <T> Future<T> submit(Runnable task, T result)
	{
	    return getBest().submit(task, result);
	}
     
    public static final void main(final String[] args) 
    {
	MultiPoolExecutor mpe = new MultiPoolExecutor();

	for(int i=0; i<12000; i++)
	    {
		TestRunnable tr = new TestRunnable();
		mpe.execute(tr);
	    }
	mpe.shutdown();
    }
    
    static class TestRunnable
	implements Runnable
    {
	static int j = 1;
	public final void run() 
	{
	    try
		{
		    
		    for(int i=0; i<100; i++)
			{
			    j += j+i + j/1000;
			    j -=  j*4/1000;
			    System.out.println(j);
			}
		    Thread.currentThread().sleep(10);			    

		    //System.out.println(j++);
		}
	    catch(Throwable t)
		{
		    t.printStackTrace();
		}
	}
    }
	


}