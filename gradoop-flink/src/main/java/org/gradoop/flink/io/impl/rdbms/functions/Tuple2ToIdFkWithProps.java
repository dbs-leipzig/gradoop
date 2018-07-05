package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

/**
 * Creates tuples of gradoop id of foreign key one, value of foreign key two and belonging properties 
 */
public class Tuple2ToIdFkWithProps implements MapFunction<Tuple2<Tuple3<String,String,Properties>,IdKeyTuple>,Tuple3<GradoopId,String,Properties>>{

	@Override
	public Tuple3<GradoopId, String, Properties> map(
			Tuple2<Tuple3<String, String, Properties>, IdKeyTuple> value) throws Exception {
		return new Tuple3<GradoopId,String,Properties>(value.f1.f0,value.f0.f1,value.f0.f2);
	}
}
