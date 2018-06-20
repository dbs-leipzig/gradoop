package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

public class VertexToIdFkTuple implements MapFunction<Vertex,IdKeyTuple> {
	String fkName;
	
	public VertexToIdFkTuple(String fkName){
		this.fkName = fkName;
	}
	@Override
	public IdKeyTuple map(Vertex v) throws Exception {
		// TODO Auto-generated method stub
		return new IdKeyTuple(v.getId(),v.getProperties().get(fkName).toString());
	}
}
