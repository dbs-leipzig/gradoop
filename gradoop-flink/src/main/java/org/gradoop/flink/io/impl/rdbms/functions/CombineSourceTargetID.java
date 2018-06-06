package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

public class CombineSourceTargetID implements MapFunction<Tuple2<Tuple3<String, GradoopId, Properties>, Tuple2<String, GradoopId>>, Tuple3<GradoopId, GradoopId, Properties>>{

	@Override
	public Tuple3<GradoopId, GradoopId, Properties> map(
			Tuple2<Tuple3<String, GradoopId, Properties>, Tuple2<String, GradoopId>> value)
					throws Exception {
		// TODO Auto-generated method stub
		return new Tuple3(value.f0.f1, value.f1.f1, value.f0.f2);
	}
}
