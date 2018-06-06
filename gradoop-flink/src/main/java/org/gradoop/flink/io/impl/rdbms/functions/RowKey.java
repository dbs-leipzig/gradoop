package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;

public class RowKey implements KeySelector<Row,String>{
	private int pos;
	
	public RowKey(int pos){
		this.pos = pos;
	}
	@Override
	public String getKey(Row row) throws Exception {
		// TODO Auto-generated method stub
		return row.getField(pos).toString();
	}

}
