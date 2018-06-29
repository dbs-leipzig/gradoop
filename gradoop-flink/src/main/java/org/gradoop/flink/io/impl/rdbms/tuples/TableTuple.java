package org.gradoop.flink.io.impl.rdbms.tuples;

import java.sql.JDBCType;

import org.apache.flink.api.java.tuple.Tuple4;

public class TableTuple extends Tuple4<String,String,Integer,JDBCType>{
	private String name;
	private String role;
	private int pos;
	private JDBCType type;
	
	public TableTuple() {
	}

	public TableTuple(String name, String role, int pos, JDBCType type) {
		super();
		this.name = name;
		this.f0 = name;
		this.role = role;
		this.f1 = role;
		this.pos = pos;
		this.f2 = pos;
		this.type = type;
		this.f3 = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public int getPos() {
		return pos;
	}

	public void setPos(int pos) {
		this.pos = pos;
	}

	public JDBCType getType() {
		return type;
	}

	public void setType(JDBCType type) {
		this.type = type;
	}
}
