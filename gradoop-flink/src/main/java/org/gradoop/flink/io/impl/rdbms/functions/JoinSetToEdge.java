package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class JoinSetToEdge extends RichMapFunction<Tuple2<Tuple3<String,GradoopId,String>,Tuple3<String,GradoopId,String>>,Edge> {
	EPGMEdgeFactory edgeFactory;
	
	public JoinSetToEdge(GradoopFlinkConfig config) {
		this.edgeFactory = config.getEdgeFactory();
	}

	@Override
	public Edge map(Tuple2<Tuple3<String,GradoopId,String>,Tuple3<String,GradoopId,String>> joinTuple) throws Exception {
		GradoopId id = GradoopId.get();
		GradoopId sourceVertexId = joinTuple.f1.f1;
		GradoopId targetVertexId = joinTuple.f0.f1;
		String label = joinTuple.f0.f0;
		return (Edge) edgeFactory.initEdge(id, label, sourceVertexId, targetVertexId);
	}
}
