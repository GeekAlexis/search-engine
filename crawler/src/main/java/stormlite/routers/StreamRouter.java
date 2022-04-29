/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stormlite.routers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.bolt.IRichBolt;
import stormlite.distributed.SenderBolt;
import stormlite.tasks.BoltTask;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;

/**
 * A StreamRouter is an internal class used to determine where
 * an item placed on a stream should go.  It doesn't actually
 * run the downstream bolt, but rather queues it up as a task.
 * 
 * @author zives
 *
 */
public abstract class StreamRouter implements OutputFieldsDeclarer {
	static Logger log = LogManager.getLogger(StreamRouter.class);
	
	List<IRichBolt> bolts;
	List<HttpURLConnection> workers;
	Fields schema;
	Set<IRichBolt> remoteBolts = new HashSet<>();
	
	public StreamRouter() {
		bolts = new ArrayList<>();
	}
	
	public StreamRouter(IRichBolt bolt) {
		this();
		bolts.add(bolt);
	}
	
	/**
	 * Add another bolt instance as a consumer of this stream
	 * 
	 * @param bolt
	 */
	public void addBolt(IRichBolt bolt) {
		bolts.add(bolt);
	}

	/**
	 * Add a sender bolt instance as a consumer of this stream
	 * 
	 * @param bolt
	 */
	public void addRemoteBolt(SenderBolt bolt) {
		bolts.add(bolt);
		remoteBolts.add(bolt);
	}
	
	/**
	 * Is this a remote (sender) bolt, or a local bolt?
	 * 
	 * @param bolt
	 * @return
	 */
	public boolean isRemoteBolt(IRichBolt bolt) {
		return remoteBolts.contains(bolt);
	}
	
	/**
	 * Add a worker node
	 * 
	 * @param worker
	 * @throws IOException
	 */
	public void addWorker(String worker) throws IOException {
		URL url = new URL(worker);
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "application/json");
		this.workers.add(conn);
	}
	
	/**
	 * The selector for the destination bolt
	 * 
	 * @param tuple
	 * @return
	 */
	protected abstract List<IRichBolt> getBoltsFor(List<Object> tuple);
	
	/**
	 * The destination bolts, as a list
	 * so we can assign each a unique position
	 * 
	 * @return
	 */
	public List<IRichBolt> getBolts() {
		return bolts;
	}
	
	/**
	 * Queues up a bolt task (for future scheduling) to process a single 
	 * stream tuple.  Will send both to remote and local nodes.
	 * 
	 * @param tuple Tuple to route
	 * @param context Overall context
	 * @param sourceExecutor (optional) ID of the sending executor
	 */
	public synchronized void execute(List<Object> tuple, TopologyContext context, String sourceExecutor) {
			List<IRichBolt> bolts = getBoltsFor(tuple);
			
			
			if (bolts != null && !bolts.isEmpty()) {
    		    for (IRichBolt bolt: bolts) {
        			log.debug("Task queued: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): " + tuple.toString());
    				context.addStreamTask(new BoltTask(bolt, new Tuple(schema, tuple, sourceExecutor)));
				}

			} else
				throw new RuntimeException("Unable to find a bolt for the tuple");
	}
	
	/**
	 * Execute a routing task for a tuple, ONLY sending to local bolt executors and NOT to remote ones
	 * 
	 * @param tuple Tuple to route
	 * @param context Overall context
	 * @param sourceExecutor (optional) ID of the sending executor
	 */
	public synchronized void executeLocally(List<Object> tuple, TopologyContext context, String sourceExecutor) {
			List<IRichBolt> bolts = getBoltsFor(tuple);
			
			for (IRichBolt bolt: bolts) {
    			// If we got a remote bolt
    			if (isRemoteBolt(bolt))
    			     continue;
//    			System.out.println("Task queued from other worker: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): " + tuple.toString());
    			log.debug("Task queued from other worker: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): " + tuple.toString());
    			if (bolt != null)
    				context.addStreamTask(new BoltTask(bolt, new Tuple(schema, tuple, sourceExecutor)));
    			else
    				throw new RuntimeException("Unable to find a bolt for the tuple");
			}
			if (bolts.isEmpty())
    			throw new RuntimeException("Unable to find a bolt for the tuple");

	}

	/**
	 * Process a tuple with fields.  Will send both to remote and local node executors.
	 * 
	 * @param tuple Tuple to route
	 * @param context Overall context
	 * @param sourceExecutor (optional) ID of the sending executor
	 */
	public synchronized void execute(Tuple tuple, TopologyContext context, String sourceExecutor) {
		execute(tuple.getValues(), context, sourceExecutor);
	}

	/**
	 * Process a tuple with fields, sending only to local node executors.
	 * 
	 * @param tuple Tuple to route
	 * @param context Overall context
	 * @param sourceExecutor (optional) ID of the sending executor
	 */
	public synchronized void executeLocally(Tuple tuple, TopologyContext context, String sourceExecutor) {
		executeLocally(tuple.getValues(), context, sourceExecutor);
	}
	/**
	 * Sets the schema of the object
	 */
	@Override
	public void declare(Fields fields) {
		schema = fields;
	}

	/**
	 * Executes all executor bolts, local and remote, with an end of stream message.  Note
	 * that EOSes are always broadcast, independent of the grouping policy.
	 * 
	 * @param context Overall context
	 * @param sourceExecutor (optional) ID of the sending executor
	 */
	public synchronized void executeEndOfStream(TopologyContext context, String sourceExecutor) {
		for (IRichBolt bolt: getBolts()) {
			log.debug("Task queued: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): (EOS)");
			context.addStreamTask(new BoltTask(bolt, Tuple.getEndOfStream(sourceExecutor)));
		}
	}

	/**
	 * Executes all executor bolts on the local machine, with an end of stream message.  Note
	 * that EOSes are always broadcast, independent of the grouping policy.
	 * 
	 * @param context Overall context
	 * @param sourceExecutor (optional) ID of the sending executor
	 */
	public synchronized void executeEndOfStreamLocally(TopologyContext context, String sourceExecutor) {
		for (IRichBolt bolt: getBolts())
			if (!isRemoteBolt(bolt)) {
				log.debug("Task queued from other worker: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): (EOS)");
				context.addStreamTask(new BoltTask(bolt, Tuple.getEndOfStream(sourceExecutor)));
			}
	}

	public String getKey(List<Object> input) {
		return input.toString();
	}
}
