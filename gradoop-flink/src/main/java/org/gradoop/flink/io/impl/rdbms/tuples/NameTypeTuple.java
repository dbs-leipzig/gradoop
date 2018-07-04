package org.gradoop.flink.io.impl.rdbms.tuples;

import java.sql.JDBCType;

import org.apache.flink.api.java.tuple.Tuple2;

public class NameTypeTuple extends Tuple2<String,JDBCType>{
	private String name;
	private JDBCType type;
	
	public NameTypeTuple(){
	}
	
	public NameTypeTuple(String name, JDBCType type) {
		this.name = name;
		this.f0 = name;
		this.type = type;
		this.f1 = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public JDBCType getType() {
		return type;
	}

	public void setType(JDBCType type) {
		this.type = type;
	}
}
