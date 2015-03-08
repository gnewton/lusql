/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     
 *     Glen Newton glen.newton@gmail.com
 */

package ca.gnewton.lusql.driver.file;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.*;



public class TraverseDirectory
    implements Runnable
{
    /**
     * Describe dir here.
     */
    private String dir;

    /**
     * Describe queue here.
     */
    private BlockingQueue<File> queue;

    /**
     * Creates a new <code>Filer</code> instance.
     *
     */
    File rootSrcDir = null;
    public TraverseDirectory(final String newRootSrcDir)
    {
	rootSrcDir = new File(newRootSrcDir);
    }

    public void run()
    {
	try
	    {
		dig(rootSrcDir);
		queue.put(new File(""));
	    }
	catch(InterruptedException ie)
	    {
		ie.printStackTrace();
	    }
    }

    
    int count = 0;
    void dig(File ff)
	throws InterruptedException
    {
	if(ff.canRead())
	    {
		queue.put(ff);
		if(ff.isDirectory())
		    {
			File files[] = ff.listFiles();
			if(files == null || files.length == 0)
			    return;
			for(int i=0; i<files.length; i++)
			    dig(files[i]);
		    }
	    }
    }


    /**
     * Get the <code>Queue</code> value.
     *
     * @return a <code>BlockingQueue</code> value
     */
    public final BlockingQueue<File> getQueue() {
	return queue;
    }

    /**
     * Set the <code>Queue</code> value.
     *
     * @param newQueue The new Queue value.
     */
    public final void setQueue(final BlockingQueue<File> newQueue) {
	this.queue = newQueue;
    }


    public static final void main(final String[] args) 
    {
	int count = 0;
	BlockingQueue<File> queue = new ArrayBlockingQueue<File>(20);
	TraverseDirectory td = null;
	if(args.length == 0)
	    td = new TraverseDirectory(".");
	else
	    td = new TraverseDirectory(args[0]);
	td.setQueue(queue);
	Thread d = new Thread(td);
	try
	    {
		d.start();
		
		File f = queue.take();
		while(!f.getName().equals(""))
		    {
			count++;
			f = queue.take();
			Thread.sleep(100);
		    }
		d.join();
	    }
	catch(Throwable t)
	    {
		t.printStackTrace();
	    }
    }

} 

