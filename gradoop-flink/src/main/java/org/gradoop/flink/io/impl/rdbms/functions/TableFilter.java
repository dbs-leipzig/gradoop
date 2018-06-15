package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

public class TableFilter implements FilterFunction<Vertex> {
	String refdTable;
	
	public TableFilter(String refdTable){
		this.refdTable = refdTable;
	}
	@Override
	public boolean filter(Vertex v) throws Exception {
		// TODO Auto-generated method stub
		return v.getLabel().equals(refdTable);
	}

}
