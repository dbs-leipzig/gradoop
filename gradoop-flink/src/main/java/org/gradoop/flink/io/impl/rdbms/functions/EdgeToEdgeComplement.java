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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Creates edges with opposite direction
 */
public class EdgeToEdgeComplement implements MapFunction<Edge,Edge> {

	@Override
	public Edge map(Edge e1) throws Exception {
		Edge e2 = new Edge();
		e2.setId(GradoopId.get());
		e2.setLabel(e1.getLabel());
		e2.setSourceId(e1.getTargetId());
		e2.setTargetId(e1.getSourceId());
		e2.setProperties(e1.getProperties());
		return e2;
	}
}
