package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

public class CreateEdges2 implements MapFunction <Tuple3<GradoopId,GradoopId,String>,Edge>{
	String attName;
	
	public CreateEdges2(String attName){
		this.attName = attName;
	}
	
	@Override
	public Edge map(Tuple3<GradoopId,GradoopId,String> in) throws Exception {
		Edge e = new Edge();
		e.setId(GradoopId.get());
		e.setLabel(attName);
		e.setSourceId(in.f0);
		e.setTargetId(in.f1);
		return e;
	}
}
