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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Tuple used to represent primary key respectively foreign key site of foreign key relation
 *
 */
public class LabelIdKeyTuple extends Tuple3<String, GradoopId, String>{
	private String label;
	private GradoopId id;
	private String key;
	
	public LabelIdKeyTuple() {
	}
	
	public LabelIdKeyTuple(String label, GradoopId id, String key) {
		this.label = label;
		this.f0 = label;
		this.id = id;
		this.f1 = id;
		this.key = key;
		this.f2 = key;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
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
