package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

public class VertexToIdPkTuple implements MapFunction<Vertex,Tuple2<GradoopId,String>> {

	@Override
	public Tuple2<GradoopId,String> map(Vertex v) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2<GradoopId, String>(v.getId(),v.getProperties().get("PrimaryKey").toString());
	}

}
