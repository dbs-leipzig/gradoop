package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

public class ReducePairs implements ReduceFunction<Tuple3<GradoopId, GradoopId, String>> {

	@Override
	public Tuple3<GradoopId, GradoopId, String> reduce(Tuple3<GradoopId, GradoopId, String> in1,
			Tuple3<GradoopId, GradoopId, String> in2) throws Exception {
		return new Tuple3<>(in1.f1,in2.f1,in2.f2);
	}

}
