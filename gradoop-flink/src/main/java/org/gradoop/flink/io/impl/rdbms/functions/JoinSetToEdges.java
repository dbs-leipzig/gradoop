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
