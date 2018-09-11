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

package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Filters a set of vertices by label
 */
public class VertexLabelFilter implements FilterFunction<Vertex> {
	/**
	 * Label to search for
	 */
	String label;
	
	/**
	 * Constructor
	 * @param label Label to search for
	 */
	public VertexLabelFilter(String label){
		this.label = label;
	}
	
	@Override
	public boolean filter(Vertex v) {
		return v.getLabel().equals(label);
	}

}
