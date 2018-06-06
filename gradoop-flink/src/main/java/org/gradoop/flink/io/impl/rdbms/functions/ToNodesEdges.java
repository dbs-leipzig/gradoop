package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;

public class ToNodesEdges implements MapFunction<Tuple2<GradoopId,GradoopId>,Edge> {
	VertexFactory vertexFactory;
	String fkName;

	public ToNodesEdges(VertexFactory vertexFactory, String fkName){
		this.vertexFactory = vertexFactory;
		this.fkName = fkName;
	}

	@Override
	public Edge map(Tuple2<GradoopId, GradoopId> tuple) throws Exception {
		// TODO Auto-generated method stub
		Edge e = new Edge();
		e.setId(GradoopId.get());
		e.setSourceId(tuple.f0);
		e.setTargetId(tuple.f1);
		e.setLabel(fkName);
		return e;
	}
}
