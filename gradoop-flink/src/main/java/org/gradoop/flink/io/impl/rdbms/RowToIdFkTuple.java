package org.gradoop.flink.io.impl.rdbms;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.gradoop.flink.io.impl.rdbms.tuples.IdKeyTuple;

public class RowToIdFkTuple implements MapFunction<Row,IdKeyTuple> {
	private String fkName;
	
	public RowToIdFkTuple(String fkName){
		this.fkName = fkName;
	}

	@Override
	public IdKeyTuple map(Row tuple) throws Exception {
		
		return new IdKeyTuple();
	}

}
