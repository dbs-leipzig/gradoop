package org.gradoop.flink.io.impl.rdbms.tuples;

import java.sql.JDBCType;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

public class FKTuple extends Tuple6<String,String,String,String,Integer,JDBCType>{
	private String refingTable;
	private String refdTable;
	private String refingAttribute;
	private String refdAttribute;
	private int pos;
	private JDBCType type;
	
	public FKTuple() {
	}

	public FKTuple(String refingTable, String refdTable, String refingAttribute, String refdAttribute,
			int pos, JDBCType type) {
		this.refingTable = refingTable;
		this.f0 = refingTable;
		this.refdTable = refdTable;
		this.f1 = refdTable;
		this.refingAttribute = refingAttribute;
		this.f2 = refingAttribute;
		this.refdAttribute = refdAttribute;
		this.f3 = refdAttribute;
		this.pos = pos;
		this.f4 = pos;
		this.type = type;
		this.f5 = type;
	}

	public String getRefingTable() {
		return refingTable;
	}

	public void setRefingTable(String refingTable) {
		this.refingTable = refingTable;
	}

	public String getRefdTable() {
		return refdTable;
	}

	public void setRefdTable(String refdTable) {
		this.refdTable = refdTable;
	}

	public String getRefingAttribute() {
		return refingAttribute;
	}

	public void setRefingAttribute(String refingAttribute) {
		this.refingAttribute = refingAttribute;
	}

	public String getRefdAttribute() {
		return refdAttribute;
	}

	public void setRefdAttribute(String refdAttribute) {
		this.refdAttribute = refdAttribute;
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
