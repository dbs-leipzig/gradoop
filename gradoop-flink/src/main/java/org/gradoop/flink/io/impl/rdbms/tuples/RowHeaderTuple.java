package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

public class RowHeaderTuple extends Tuple3<String,String,Integer>{
	private String name;
	private String attType;
	private int pos;
	
	public RowHeaderTuple(String name, String attType, int pos){
		this.name=name;
		this.f0 = name;
		this.attType = attType;
		this.f1= attType;
		this.pos = pos;
		this.f2 = pos;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAttType() {
		return attType;
	}

	public void setAttType(String attType) {
		this.attType = attType;
	}

	public int getPos() {
		return pos;
	}

	public void setPos(int pos) {
		this.pos = pos;
	}
}
