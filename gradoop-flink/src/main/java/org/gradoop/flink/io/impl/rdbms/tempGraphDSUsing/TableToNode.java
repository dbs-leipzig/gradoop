package org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing;

import java.sql.JDBCType;
import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.connection.SQLToBasicTypeMapper;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.metadata.SQLQuery;
import org.gradoop.flink.io.impl.rdbms.tuples.AttTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

import com.sun.tools.javac.main.Option.PkgInfo;

public class TableToNode {
	private String tableName;
	private ArrayList<NameTypeTuple> primaryKeys;
	private ArrayList<NameTypeTuple> foreignKeys;
	private ArrayList<NameTypeTuple> furtherAttributes;
	private int rowCount;
	private String sqlQuery;
	private RowHeader rowheader;
	
	public TableToNode(String tableName, ArrayList<NameTypeTuple> primaryKeys, ArrayList<NameTypeTuple> foreignKeys, ArrayList<NameTypeTuple> furtherAttributes,
			int rowCount) {
		this.tableName = tableName;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
		this.furtherAttributes = furtherAttributes;
		this.rowCount = rowCount;
		this.sqlQuery = SQLQuery.getNodeTableQuery(tableName, primaryKeys, foreignKeys, furtherAttributes);
		this.rowheader = new RowHeader();
	}
	
	public RowTypeInfo getRowTypeInfo(){
		int i = 0;
		TypeInformation[] fieldTypes = new TypeInformation[primaryKeys.size()+furtherAttributes.size()];
		
		for(NameTypeTuple pk : primaryKeys){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(pk.f1);
			rowheader.getRowHeader().add(new RowHeaderTuple(pk.f0,RDBMSConstants.PK_FIELD,i));
			i++;
		}
		for(NameTypeTuple fk : foreignKeys){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(fk.f1);
			rowheader.getRowHeader().add(new RowHeaderTuple(fk.f0,RDBMSConstants.FK_FIELD,i));
			i++;
		}
		for(NameTypeTuple att : furtherAttributes){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(att.f1);
			rowheader.getRowHeader().add(new RowHeaderTuple(att.f0,RDBMSConstants.ATTRIBUTE_FIELD,i));
			i++;
		}
		return new RowTypeInfo(fieldTypes);
	}
	
	public ArrayList<NameTypeTuple> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(ArrayList<NameTypeTuple> foreignKeys) {
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

	public ArrayList<NameTypeTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<NameTypeTuple> furtherAttributes) {
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
