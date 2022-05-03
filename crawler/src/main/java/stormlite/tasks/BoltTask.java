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
package stormlite.tasks;

import stormlite.bolt.IRichBolt;
import stormlite.tuple.Tuple;

/**
 * This is a simple task that, when scheduled, processes
 * a tuple through a bolt.
 * 
 * @author zives
 *
 */
public class BoltTask implements ITask {
	
	IRichBolt bolt;
	Tuple tuple;
	
	public BoltTask(IRichBolt bolt, Tuple tuple) {
		this.bolt = bolt;
		this.tuple = tuple;
	}

	@Override
	public void run() {
		synchronized (bolt) {
			bolt.execute(tuple);
		}
	}

	@Override
	public String getStream() {
		return bolt.getExecutorId();
	}

	@Override
	public String toString() {
		return bolt.getClass().getName() + "/" + bolt.getExecutorId() + ": " + tuple.toString();
	}
}
