package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

public class VertexToIdPkTuple implements MapFunction<Vertex,IdKeyTuple> {

	@Override
	public IdKeyTuple map(Vertex v) throws Exception {
		// TODO Auto-generated method stub
		return new IdKeyTuple(v.getId(),v.getProperties().get("PrimaryKey").toString());
	}

}
