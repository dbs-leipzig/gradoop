package org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing;

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

public class TablesToEdges {
	private String relationshipType;
	private String startTable;
	private String endTable;
	private ArrayList<PKTuple> primaryKeys;
	private NameTypeTuple startAttribute;
	private NameTypeTuple endAttribute;
	private boolean directionIndicator;
	private ArrayList<AttTuple> furtherAttributes;
	private int rowCount;
	private String sqlQuery;
	private RowHeader rowheader;

	public TablesToEdges(String relationshipType, String startTable, String endTable,ArrayList<PKTuple> primaryKeys, NameTypeTuple startAttribute,
			NameTypeTuple endAttribute, boolean directionIndicator, ArrayList<AttTuple> furtherAttributes, int rowCount) {
		this.relationshipType = relationshipType;
		this.startTable = startTable;
		this.primaryKeys = primaryKeys;
		this.endTable = endTable;
		this.startAttribute = startAttribute;
		this.endAttribute = endAttribute;
		this.directionIndicator = directionIndicator;
		this.furtherAttributes = furtherAttributes;
		this.rowCount = rowCount;
		this.rowheader = new RowHeader();
		if (directionIndicator) {
			this.sqlQuery = SQLQuery.getStandardEdgeTableQuery(startTable, primaryKeys, endAttribute.f0);
		} else {
			this.sqlQuery = SQLQuery.getNtoMEdgeTableQuery(relationshipType, startAttribute.f0, endAttribute.f0,
					furtherAttributes);
		}
	}

	public RowTypeInfo getRowTypeInfo() {
		TypeInformation[] fieldTypes = null;

		if (directionIndicator) {
			int i = 0;
			fieldTypes = new TypeInformation[primaryKeys.size()+1];

			for (PKTuple pk : primaryKeys) {
				fieldTypes[i] = SQLToBasicTypeMapper.getBasicTypeInfo(pk.f3);
				rowheader.getRowHeader().add(new RowHeaderTuple(pk.getName(),RDBMSConstants.PK_FIELD,i));
				i++;
			}
			fieldTypes[i] = SQLToBasicTypeMapper.getBasicTypeInfo(endAttribute.f1);
			rowheader.getRowHeader().add(new RowHeaderTuple(endAttribute.f0,RDBMSConstants.ATTRIBUTE_FIELD,i));

		} else {
			int i = 2;
			fieldTypes = new TypeInformation[furtherAttributes.size()+2];
			fieldTypes[0] = SQLToBasicTypeMapper.getBasicTypeInfo(startAttribute.f1);
			fieldTypes[1] = SQLToBasicTypeMapper.getBasicTypeInfo(endAttribute.f1);
			for (AttTuple att : furtherAttributes) {
				fieldTypes[i] = SQLToBasicTypeMapper.getBasicTypeInfo(att.f3);
				rowheader.getRowHeader().add(new RowHeaderTuple(att.f0,RDBMSConstants.ATTRIBUTE_FIELD,i));
				i++;
			}
		}
		return new RowTypeInfo(fieldTypes);
	}

	/*
	 * Getter and Setter
	 */
	
	public NameTypeTuple getStartAttribute() {
		return startAttribute;
	}

	public RowHeader getRowheader() {
		return rowheader;
	}

	public void setRowheader(RowHeader rowheader) {
		this.rowheader = rowheader;
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

	public ArrayList<PKTuple> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(ArrayList<PKTuple> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public String getRelationshipType() {
		return relationshipType;
	}

	public void setRelationshipType(String relationshipType) {
		this.relationshipType = relationshipType;
	}

	public String getStartTable() {
		return startTable;
	}

	public void setStartTable(String startTable) {
		this.startTable = startTable;
	}

	public String getEndTable() {
		return endTable;
	}

	public void setEndTable(String endTable) {
		this.endTable = endTable;
	}

	public boolean isDirectionIndicator() {
		return directionIndicator;
	}

	public void setDirectionIndicator(boolean directionIndicator) {
		this.directionIndicator = directionIndicator;
	}

	public ArrayList<AttTuple> getFurtherAttributes() {
		return furtherAttributes;
	}

	public void setFurtherAttributes(ArrayList<AttTuple> furtherAttributes) {
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
