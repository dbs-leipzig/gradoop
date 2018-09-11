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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.rdbms.tuples.LabelIdKeyTuple;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Creates edges from joined primary key respectively foreign key tables of foreign key relations
 *
 */
public class JoinSetToEdges extends RichMapFunction<Tuple2<LabelIdKeyTuple,LabelIdKeyTuple>,Edge> {
	EPGMEdgeFactory edgeFactory;
	
	public JoinSetToEdges(GradoopFlinkConfig config) {
		this.edgeFactory = config.getEdgeFactory();
	}

	@Override
	public Edge map(Tuple2<LabelIdKeyTuple,LabelIdKeyTuple> preEdge) throws Exception {
		GradoopId id = GradoopId.get();
		GradoopId sourceVertexId = preEdge.f1.f1;
		GradoopId targetVertexId = preEdge.f0.f1;
		String label = preEdge.f0.f0;
		return (Edge) edgeFactory.initEdge(id, label, sourceVertexId, targetVertexId);
	}
}
