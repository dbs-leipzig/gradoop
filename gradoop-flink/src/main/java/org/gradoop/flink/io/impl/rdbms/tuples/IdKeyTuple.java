/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Tuple representing a gradoop id, key string pair
 */
public class IdKeyTuple extends Tuple2<GradoopId, String> {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Gradoop id
	 */
	private GradoopId id;
	
	/**
	 * Key string
	 */
	private String key;

	public IdKeyTuple() {
		
	}
	
	/**
	 * Constructor
	 * @param id Gradoop id
	 * @param key Key string
	 */
	public IdKeyTuple(GradoopId id, String key) {
		this.id = id;
		this.f0 = id;
		this.key = key;
		this.f1 = key;
	}

	public GradoopId getId() {
		return id;
	}

	public void setId(GradoopId id) {
		this.id = id;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
