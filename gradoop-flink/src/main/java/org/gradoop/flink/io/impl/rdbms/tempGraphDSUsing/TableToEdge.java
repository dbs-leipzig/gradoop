package org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing;

import java.util.ArrayList;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.connection.SQLToBasicTypeMapper;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.metadata.RowHeader;
import org.gradoop.flink.io.impl.rdbms.metadata.SQLQuery;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

public class TableToEdge {
	private String relationshipType;
	private String startTableName;
	private String endTableName;
	private NameTypeTuple startAttribute;
	private NameTypeTuple endAttribute;
	private ArrayList<NameTypeTuple> primaryKeys;
	private ArrayList<NameTypeTuple> furtherAttributes;
	private boolean directionIndicator;
	private int rowCount;
	private String sqlQuery;
	private RowHeader rowheader;

	public TableToEdge(String relationshipType, String startTableName, String endTableName, NameTypeTuple startAttribute,
			NameTypeTuple endAttribute, ArrayList<NameTypeTuple> primaryKeys,
			ArrayList<NameTypeTuple> furtherAttributes, boolean directionIndicator, int rowCount) {
		this.relationshipType = relationshipType;
		this.startTableName = startTableName;
		this.endTableName = endTableName;
		this.startAttribute = startAttribute;
		this.endAttribute = endAttribute;
		this.primaryKeys = primaryKeys;
		this.furtherAttributes = furtherAttributes;
		this.directionIndicator = directionIndicator;
		this.rowCount = rowCount;
		this.rowheader = new RowHeader();
		if (!directionIndicator) {
			this.sqlQuery = SQLQuery.getNtoMEdgeTableQuery(relationshipType, startAttribute.f0, endAttribute.f0,
					furtherAttributes);
		}
	}

	public RowTypeInfo getRowTypeInfo() {
		TypeInformation[] fieldTypes = null;

		if (directionIndicator) {
			int i = 0;
			fieldTypes = new TypeInformation[primaryKeys.size() + 1];

			for (NameTypeTuple pk : primaryKeys) {
				fieldTypes[i] = SQLToBasicTypeMapper.getBasicTypeInfo(pk.f1);
				rowheader.getRowHeader().add(new RowHeaderTuple(pk.getName(), RDBMSConstants.PK_FIELD, i));
				i++;
			}
			fieldTypes[i] = SQLToBasicTypeMapper.getBasicTypeInfo(endAttribute.f1);
			rowheader.getRowHeader().add(new RowHeaderTuple(endAttribute.f0, RDBMSConstants.ATTRIBUTE_FIELD, i));

		} else {
			int i = 2;
			fieldTypes = new TypeInformation[furtherAttributes.size() + 2];
			fieldTypes[0] = SQLToBasicTypeMapper.getBasicTypeInfo(startAttribute.f1);
			rowheader.getRowHeader().add(new RowHeaderTuple(startAttribute.f0,RDBMSConstants.FK_FIELD,0));
			fieldTypes[1] = SQLToBasicTypeMapper.getBasicTypeInfo(endAttribute.f1);
			rowheader.getRowHeader().add(new RowHeaderTuple(endAttribute.f0,RDBMSConstants.FK_FIELD,1));
			for (NameTypeTuple att : furtherAttributes) {
				fieldTypes[i] = SQLToBasicTypeMapper.getBasicTypeInfo(att.f1);
				rowheader.getRowHeader().add(new RowHeaderTuple(att.f0, RDBMSConstants.ATTRIBUTE_FIELD, i));
				i++;
			}
		}
		return new RowTypeInfo(fieldTypes);
	}

	/*
	 * Getter and Setter
	 */
	public String getstartTableName() {
		return startTableName;
	}

	public String getRelationshipType() {
		return relationshipType;
	}

	public void setRelationshipType(String relationshipType) {
		this.relationshipType = relationshipType;
	}

	public void setstartTableName(String startTableName) {
		this.startTableName = startTableName;
	}

	public String getendTableName() {
		return endTableName;
	}

	public void setendTableName(String endTableName) {
		this.endTableName = endTableName;
	}

	public NameTypeTuple getStartAttribute() {
		return startAttribute;
	}

	public void setStartAttribute(NameTypeTuple startAttribute) {
		this.startAttribute = startAttribute;
	}

	public NameTypeTuple getEndAttribute() {
		return endAttribute;
	}

	public void setEndAttribute(NameTypeTuple endAttribute) {
		this.endAttribute = endAttribute;
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

	public boolean isDirectionIndicator() {
		return directionIndicator;
	}

	public void setDirectionIndicator(boolean directionIndicator) {
		this.directionIndicator = directionIndicator;
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

	public RowHeader getRowheader() {
		return rowheader;
	}

	public void setRowheader(RowHeader rowheader) {
		this.rowheader = rowheader;
	}
}
