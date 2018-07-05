package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Represents a tuple of a rowheader
 */
public class RowHeaderTuple extends Tuple3<String,String,Integer>{
	
	/**
	 * Name of attribute
	 */
	private String name;
	
	/**
	 * Data type of attribute
	 */
	private String attType;
	
	/**
	 * Position of attribute in row
	 */
	private int pos;
	
	public RowHeaderTuple() {
		
	}
	
	/**
	 * Constructor
	 * @param name Attribute name
	 * @param attType Datatype of attribute
	 * @param pos Position of attribute in row
	 */
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
