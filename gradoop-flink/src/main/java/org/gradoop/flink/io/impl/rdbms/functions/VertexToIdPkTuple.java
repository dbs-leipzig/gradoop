package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

/**
 * Creates pairs consisting of gradoop id and primary key name, from vertices
 */
public class VertexToIdPkTuple implements MapFunction<Vertex,IdKeyTuple> {
	
	private String refdAttName;
	
	public VertexToIdPkTuple(String refdAttName) {
		this.refdAttName = refdAttName;
	}

	@Override
	public IdKeyTuple map(Vertex v) throws Exception {
		return new IdKeyTuple(v.getId(),v.getProperties().get(refdAttName).toString());
	}
}
