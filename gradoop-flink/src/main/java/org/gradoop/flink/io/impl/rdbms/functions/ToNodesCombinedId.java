package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.rdbms.tuples.PkId;

public class ToNodesCombinedId implements MapFunction<Tuple2<PkId, PkId>, Tuple2<GradoopId, GradoopId>> {

	@Override
	public Tuple2<GradoopId, GradoopId> map(Tuple2<PkId,PkId> value)
					throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2(value.f0.f1, value.f1.f1);
	}
}
