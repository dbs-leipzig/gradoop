package org.gradoop.flink.io.impl.rdbms.tuples;

import java.util.ArrayList;

import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;

public class RowHeader implements Cloneable{
	private ArrayList<RowHeaderTuple> rowHeader;
	
	public RowHeader(){
		rowHeader = new ArrayList<RowHeaderTuple>();
	}

	public RowHeader(ArrayList<RowHeaderTuple> rowHeader) {
		this.rowHeader = rowHeader;
	}

	public ArrayList<RowHeaderTuple> getRowHeader() {
		return rowHeader;
	}

	public void setRowHeader(ArrayList<RowHeaderTuple> rowHeader) {
		this.rowHeader = rowHeader;
	}
	
	public ArrayList<RowHeaderTuple> getForeignKeyHeader(){
		ArrayList<RowHeaderTuple> fkHeader = new ArrayList<RowHeaderTuple>();
		for(RowHeaderTuple rht : this.rowHeader){
			if(rht.getAttType().equals(RDBMSConstants.FK_FIELD)){
				fkHeader.add(rht);
			}
		}
		return fkHeader;
	}
}
