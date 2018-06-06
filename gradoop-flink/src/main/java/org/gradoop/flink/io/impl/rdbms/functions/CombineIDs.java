package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.Vertex;

public class CombineIDs implements JoinFunction<Row,Row,Row>, KeySelector{

	@Override
	public Row join(Row first, Row second) throws Exception {
		// TODO Auto-generated method stub
		Row r = new Row(first.getArity()+1);
		for(int i = 0; i < first.getArity();i++){
			r.setField(i, first.getField(i));
		}
		r.setField(first.getArity(), second.getId());
		return r;
	}
}
