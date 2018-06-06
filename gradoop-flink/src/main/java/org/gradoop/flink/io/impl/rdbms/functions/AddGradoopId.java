package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;

public class AddGradoopId implements MapFunction<Row,Row>{
	@Override
	public Row map(Row tuple) throws Exception {
		// TODO Auto-generated method stub
		int tLength = tuple.getArity();
		Row r = new Row(tLength + 1);
		for (int i = 0; i < tLength; i++) {
			r.setField(i, tuple.getField(i));
		}
		r.setField(tLength, GradoopId.get());
		return r;
	}
}
