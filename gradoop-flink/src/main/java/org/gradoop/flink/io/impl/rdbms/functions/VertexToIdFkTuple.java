package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

public class VertexToIdFkTuple implements MapFunction<Vertex,Tuple2<GradoopId,String>> {
	String fkName;
	
	public VertexToIdFkTuple(String fkName){
		this.fkName = fkName;
	}
	@Override
	public Tuple2<GradoopId,String> map(Vertex v) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2(v.getId(),v.getProperties().get(fkName).toString());
	}
}
