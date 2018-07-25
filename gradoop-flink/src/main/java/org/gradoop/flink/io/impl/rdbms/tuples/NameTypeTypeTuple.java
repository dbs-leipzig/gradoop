package org.gradoop.flink.io.impl.rdbms.tuples;

import java.sql.JDBCType;

import org.apache.flink.api.java.tuple.Tuple3;

public class NameTypeTypeTuple extends Tuple3<String,JDBCType,JDBCType>{
	private String name;
	private JDBCType jdbctype;
	private JDBCType nestedType;
	
	public NameTypeTypeTuple(){
		
	}

	public NameTypeTypeTuple(String name, JDBCType jdbctype, JDBCType nestedType) {
		this.name = name;
		this.jdbctype = jdbctype;
		this.nestedType = nestedType;
		
		this.f0 = name;
		this.f1 = jdbctype;
		this.f2 = nestedType;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public JDBCType getJdbctype() {
		return jdbctype;
	}

	public void setJdbctype(JDBCType jdbctype) {
		this.jdbctype = jdbctype;
	}

	public JDBCType getNestedType() {
		return nestedType;
	}

	public void setNestedType(JDBCType nestedType) {
		this.nestedType = nestedType;
	}
}
