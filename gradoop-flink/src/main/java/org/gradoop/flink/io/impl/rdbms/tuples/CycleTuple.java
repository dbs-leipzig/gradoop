package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

public class CycleTuple {
	public String tableName;
	public Integer visited;
	public Integer cycle;
	
	public CycleTuple(String tableName, Integer visited, Integer cycle){
		this.tableName = tableName;
		this.visited = visited;
		this.cycle = cycle;
	}
}
