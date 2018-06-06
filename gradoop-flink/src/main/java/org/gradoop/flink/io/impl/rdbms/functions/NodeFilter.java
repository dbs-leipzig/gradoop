package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class NodeFilter implements FilterFunction<Tuple2<String,String>>{
	private String refdTable;
	
	public NodeFilter(String refdTable){
		this.refdTable = refdTable;
	}
	@Override
	public boolean filter(Tuple2<String, String> value) throws Exception {
		// TODO Auto-generated method stub
		return value.f1.equals(refdTable);
	}

}
