package org.gradoop.flink.io.impl.rdbms.tuples;

import java.sql.JDBCType;

import org.apache.flink.api.java.tuple.Tuple4;

public class FkTuple extends Tuple4<String,JDBCType,String,String>{

	/**
	 * Foreign key name
	 */
	private String fkName;

	/**
	 * JDBC data type of foreign key name
	 */
	private JDBCType type;
	
	/**
	 * Column name of referenced table
	 */
	private String refdAttName;

	/**
	 * Name of referenced Table
	 */
	private String refdTableName;
	
	public FkTuple() {

	}

	/**
	 * Constructor
	 * @param name Key string
	 * @param type JDBC data type
	 */
	public FkTuple(String fkName, JDBCType type, String refdAttName, String refdTableName) {
		this.fkName = fkName;
		this.f0 = fkName;
		this.type = type;
		this.f1 = type;
		this.refdAttName = refdAttName;
		this.f2 = refdAttName;
		this.refdTableName = refdTableName;
		this.f3 = refdTableName;
	}

	public String getFkName() {
		return fkName;
	}

	public void setFkName(String fkName) {
		this.fkName = fkName;
	}

	public JDBCType getType() {
		return type;
	}

	public void setType(JDBCType type) {
		this.type = type;
	}

	public String getRefdAttName() {
		return refdAttName;
	}

	public void setRefdAttName(String refdAttName) {
		this.refdAttName = refdAttName;
	}

	public String getRefdTableName() {
		return refdTableName;
	}

	public void setRefdTableName(String refdTableName) {
		this.refdTableName = refdTableName;
	}
}
