package org.gradoop.flink.io.impl.rdbms.metadata;

import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.connection.SQLToBasicTypeMapper;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

/**
 * Stores metadata for tuple-to-vertex conversation
 */
public class TableToNode {

	/**
	 * Name of database table
	 */
	private String tableName;

	/**
	 * List of primary key names and belonging datatypes
	 */
	private ArrayList<NameTypeTuple> primaryKeys;

	/**
	 * List of foreign key names and belonging datatypes
	 */
	private ArrayList<FkTuple> foreignKeys;

	/**
	 * List of further attribute names and belonging datatypes
	 */
	private ArrayList<NameTypeTypeTuple> furtherAttributes;

	/**
	 * Numbe of rows of database table
	 */
	private int rowCount;

	/**
	 * Valid sql query for querying needed relational data
	 */
	private String sqlQuery;

	/**
	 * Rowheader for row data representation of relational data
	 */
	private RowHeader rowheader;

	/**
	 * Constructor
	 * @param tableName Name of database table
	 * @param primaryKeys List of primary key names and datatypes
	 * @param foreignKeys List of foreign key names and datatypes
	 * @param furtherAttributes List of further attribute names and datatypes
	 * @param rowCount Number of database rows
	 */
	public TableToNode(String tableName, ArrayList<NameTypeTuple> primaryKeys, ArrayList<FkTuple> foreignKeys, ArrayList<NameTypeTypeTuple> furtherAttributes,
			int rowCount) {
		this.tableName = tableName;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.furtherAttributes = furtherAttributes;
		this.rowCount = rowCount;
		this.sqlQuery = SQLQuery.getNodeTableQuery(tableName, primaryKeys, foreignKeys, furtherAttributes);
		this.rowheader = new RowHeader();
	}

	/**
	 * Creates a valid type information for belonging sql query
	 * @return Row type information for belonging sql query
	 */
	public RowTypeInfo getRowTypeInfo(){
		int i = 0;
		TypeInformation[] fieldTypes = new TypeInformation[primaryKeys.size()+foreignKeys.size()+furtherAttributes.size()];

		try {
		for(NameTypeTuple pk : primaryKeys){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(pk.f1,null);
			rowheader.getRowHeader().add(new RowHeaderTuple(pk.f0,RdbmsConstants.PK_FIELD,i));
			i++;
		}
		for(FkTuple fk : foreignKeys){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(fk.f1,null);
			rowheader.getRowHeader().add(new RowHeaderTuple(fk.f0,RdbmsConstants.FK_FIELD,i));
			i++;
		}
		for(NameTypeTypeTuple att : furtherAttributes){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(att.f1,att.f2);
			rowheader.getRowHeader().add(new RowHeaderTuple(att.f0,RdbmsConstants.ATTRIBUTE_FIELD,i));
			i++;
		}
		}catch(Exception e) {
			
		}
	
		return new RowTypeInfo(fieldTypes);
	}

	public ArrayList<FkTuple> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(ArrayList<FkTuple> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	public RowHeader getRowheader() {
		return rowheader;
	}

	public void setRowheader(RowHeader rowheader) {
		this.rowheader = rowheader;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<NameTypeTuple> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(ArrayList<NameTypeTuple> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public ArrayList<NameTypeTypeTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<NameTypeTypeTuple> furtherAttributes) {
		this.furtherAttributes = furtherAttributes;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}
}
