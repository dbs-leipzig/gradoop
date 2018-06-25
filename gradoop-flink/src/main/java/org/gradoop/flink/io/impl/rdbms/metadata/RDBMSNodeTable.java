package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.tuples.RowHeaderTuple;

public class RDBMSNodeTable extends RDBMSTableBase{
	
	public RDBMSNodeTable(){
		super();
	}
	
	public String getNodeSqlQuery() {
		super.getjdbcTypes().clear();
		
		String sql = "SELECT ";
		
		int i = 0;
		for (String pk : super.getPrimaryKey()) {
			sql = sql + pk + ",";
			RowHeaderTuple rht = new RowHeaderTuple(pk, "pk", i);
			super.getRowHeader().getRowHeader().add(rht);
			super.getjdbcTypes().add(super.getAttributes().get(pk));
			i++;
		}
		for (Entry<String, String> fk : super.getForeignKeys().entrySet()) {
			sql += fk.getKey() + ",";
			RowHeaderTuple rht = new RowHeaderTuple(fk.getKey(), "fk", i);
			super.getRowHeader().getRowHeader().add(rht);
			super.getjdbcTypes().add(super.getAttributes().get(fk.getKey()));
			i++;
		}
		for (String att : getSimpleAttributes()) {
			sql += att + ",";
			RowHeaderTuple rht = new RowHeaderTuple(att, "att", i);
			super.getRowHeader().getRowHeader().add(rht);
			super.getjdbcTypes().add(super.getAttributes().get(att));
			i++;
		}
		return sql = sql.substring(0, sql.length() - 1) + " FROM " + super.getTableName();
	}
	
	/**
	 * collects all non primary key and non foreign key attributes
	 */
	public ArrayList<String> getSimpleAttributes() {
		ArrayList<String> simpleAttributes = new ArrayList<String>();
		HashSet<String> fkAttributes = new HashSet<String>();
		HashSet<String> pkAttributes = new HashSet<String>();

		for (String pk : super.getPrimaryKey()) {
			pkAttributes.add(pk);
		}
		for (Entry<String, String> fk : super.getForeignKeys().entrySet()) {
			fkAttributes.add(fk.getKey());
		}
		for (Entry<String, JDBCType> attr : super.getAttributes().entrySet()) {
			if (!fkAttributes.contains(attr.getKey()) && !pkAttributes.contains(attr.getKey())) {
				simpleAttributes.add(attr.getKey());
			}
		}
		return simpleAttributes;
	}
}
