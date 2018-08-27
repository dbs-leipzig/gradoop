package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;

public class EdgeIdMapper implements MapFunction<Edge,Edge> {
	@Override
	public Edge map(Edge e) throws Exception {
		return e;
	}

}
