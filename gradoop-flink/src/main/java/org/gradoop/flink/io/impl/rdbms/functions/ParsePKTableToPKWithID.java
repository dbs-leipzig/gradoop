package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.rdbms.tuples.FKEdge;

public class ParsePKTableToPKWithID implements MapFunction<Row,FKEdge>{

	int pos;
	
	public ParsePKTableToPKWithID(int pos){
		this.pos = pos;
	}
	
	@Override
	public FKEdge map(Row row) throws Exception {
		// TODO Auto-generated method stub
		return new FKEdge(row.getField(pos).toString(),(GradoopId)row.getField(row.getArity()-1));
	}

}
