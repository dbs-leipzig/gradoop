package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;

import akka.japi.tuple.Tuple4;

public class VertexToTuple4 implements MapFunction<Vertex, Tuple4<String,GradoopId,GradoopId,Properties>> {

	@Override
	public Tuple4<String, GradoopId, GradoopId, Properties> map(Vertex v) throws Exception {
		return new Tuple4<>(v.getLabel(),GradoopId.get(),v.getId(),v.getProperties());
	}

}
