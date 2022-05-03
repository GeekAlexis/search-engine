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
package stormlite.tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Simple tuple class
 *
 * @author zives
 *
 */
public class Tuple implements Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private Fields fields;
	private List<Object> values;

	/**
	 * Primarily for your own debug purposes we will track
	 * the source node for each tuple
	 */
	private String sourceExecutor;

	private boolean endOfStream = false;

	private Tuple() {
		endOfStream = true;
		values = null;
	}

	/**
	 * Returns a special tuple indicating end of stream
	 *
	 * @return
	 */
	public static Tuple getEndOfStream(String sourceExecutor) {
		Tuple t = new Tuple();
		t.setSourceExecutor(sourceExecutor);
		return t;
	}

	/**
	 * Initialize tuple with list of fields and values
	 *
	 * @param fields2
	 * @param tuple
	 */
	public Tuple(Fields fields2, List<Object> tuple, String source) {
		fields = fields2;

		values = tuple;

		sourceExecutor = source;

		if (fields != null && fields.size() != values.size())
			throw new IllegalArgumentException("Cardinality mismatch between fields and values");
	}

	/**
	 * Initialize a unary tuple with a field name
	 *
	 * @param fieldName
	 * @param value
	 */
	public Tuple(String fieldName, Object value, String source) {
		fields = new Fields(fieldName);

		values = new ArrayList<>();
		values.add(value);

		sourceExecutor = source;
	}

	/**
	 * The Fields we are representing
	 * @return
	 */
	public Fields getFields() {
		return fields;
	}

	public void setFields(Fields fields) {
		this.fields = fields;
	}

	/**
	 * Values, in list order
	 *
	 * @return
	 */
	public List<Object> getValues() {
		return values;
	}

	public void setValues(List<Object> values) {
		this.values = values;
	}

	public Object getObjectByField(String string) {
		int i = fields.indexOf(string);

		if (i < 0)
			return null;
		else
			return values.get(i);
	}

	public String getStringByField(String string) {
		return (String)getObjectByField(string);
	}

	public Integer getIntegerByField(String string) {
		return (Integer)getObjectByField(string);
	}

	public String toString() {
		if (isEndOfStream()) {
			return "(EOS)";
		}

		StringBuilder ret = new StringBuilder();

		ret.append('{');
		for (int i = 0; i < values.size(); i++) {
			if (i > 0)
				ret.append(',');

			ret.append(fields.get(i));
			ret.append(": ");
			ret.append(values.get(i));
		}
		ret.append('}');

		return ret.toString();
	}

	@JsonIgnore
	public Object getHead() {
		return values.get(0);
	}

	@JsonIgnore
	public List<Object> getTail() {
		return values.subList(1, values.size());
	}


	public String getSourceExecutor() {
		return sourceExecutor;
	}

	public void setSourceExecutor(String sourceExecutor) {
		this.sourceExecutor = sourceExecutor;
	}

	public boolean isEndOfStream() {
		return endOfStream;
	}

	public void setEndOfStream(boolean eos) {
		endOfStream = eos;
	}
}
