package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

public class VertexIdMapper implements MapFunction<Vertex,Vertex> {
	@Override
	public Vertex map(Vertex v) throws Exception {
		return v;
	}

}
