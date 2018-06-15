package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Properties;

public class Tuple3ToEdge implements MapFunction<Tuple2<Tuple3<GradoopId,String,Properties>,Tuple2<GradoopId,String>>,Edge> {
	String tableName;
	
	public Tuple3ToEdge(String tableName){
		this.tableName = tableName;
	}
	
	@Override
	public Edge map(Tuple2<Tuple3<GradoopId, String, Properties>, Tuple2<GradoopId, String>> value) throws Exception {
		// TODO Auto-generated method stub
		Edge e = new Edge();
		e.setId(GradoopId.get());
		e.setSourceId(value.f0.f0);
		e.setTargetId(value.f1.f0);
		e.setProperties(value.f0.f2);
		e.setLabel(tableName);
		return e;
	}

}
