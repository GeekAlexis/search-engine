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

import stormlite.bolt.IRichBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Indicates that we should broadcast to all children
 * 
 * @author zives
 *
 */
public class Broadcast extends IStreamRouter {
	static Logger log = LogManager.getLogger(Broadcast.class);
	
	int inx = 0;
	List<IRichBolt> children;
	
	public Broadcast() {
		children = new ArrayList<IRichBolt>();
	}
	
	public Broadcast(IRichBolt child) {
		children = new ArrayList<IRichBolt>();
		children.add(child);
	}
	
	public Broadcast(List<IRichBolt> children) {
		this.children = children;
	}
	

	/**
	 * Broadcast to all of the bolts
	 * 
	 */
	@Override
	protected List<IRichBolt> getBoltsFor(List<Object> tuple) {
		
		if (getBolts().isEmpty()) {
			log.error("Could not find destination for " + tuple.toString());
			return null;
		}
		
		return getBolts();
	}


}
