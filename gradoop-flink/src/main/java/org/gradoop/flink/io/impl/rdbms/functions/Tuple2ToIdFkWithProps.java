package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

public class Tuple2ToIdFkWithProps implements MapFunction<Tuple2<Tuple3<String,String,Properties>,Tuple2<GradoopId,String>>,Tuple3<GradoopId,String,Properties>>{

	@Override
	public Tuple3<GradoopId, String, Properties> map(
			Tuple2<Tuple3<String, String, Properties>, Tuple2<GradoopId, String>> value) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple3(value.f1.f0,value.f0.f1,value.f0.f2);
	}

}
