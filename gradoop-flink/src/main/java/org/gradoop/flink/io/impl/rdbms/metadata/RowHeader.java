package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;

import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * Rowheader of a row based relational data representation
 */
public class RowHeader {
	
	/**
	 * List of single rowheader tuples
	 */
	private ArrayList<RowHeaderTuple> rowHeader;
	
	/**
	 * Empty constructor
	 */
	public RowHeader(){
		rowHeader = new ArrayList<RowHeaderTuple>();
	}

	/**
	 * Constructor
	 * @param rowHeader List of rowheader tuples
	 */
	public RowHeader(ArrayList<RowHeaderTuple> rowHeader) {
		this.rowHeader = rowHeader;
	}

	/**
	 * Collects just rowheader tuples of foreign key attributes
	 * @return List of rowheader tuples of foreign key attributes
	 */
	public ArrayList<RowHeaderTuple> getForeignKeyHeader(){
		ArrayList<RowHeaderTuple> fkHeader = new ArrayList<RowHeaderTuple>();
		for(RowHeaderTuple rht : this.rowHeader){
			if(rht.getAttType().equals(RDBMSConstants.FK_FIELD)){
				fkHeader.add(rht);
			}
		}
		return fkHeader;
	}
	
	public ArrayList<RowHeaderTuple> getRowHeader() {
		return rowHeader;
	}

	public void setRowHeader(ArrayList<RowHeaderTuple> rowHeader) {
		this.rowHeader = rowHeader;
	}
}
