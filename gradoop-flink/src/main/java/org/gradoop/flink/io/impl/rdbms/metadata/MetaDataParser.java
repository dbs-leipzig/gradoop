/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.rdbms.functions.TableRowSize;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;

/**
 * Relational database schema.
 */
public class MetaDataParser {
	
	/**
	 * Database connection
	 */
	private Connection con;
	
	/**
	 * Relational database metadata
	 */
	private DatabaseMetaData metadata;
	
	/**
	 * Parsed relational database tables
	 */
	private ArrayList<RDBMSTableBase> tableBase;

	/**
	 * Parses the schema of a relational database and provides base classes for graph conversation
	 * @param con Database connection
	 * @throws SQLException
	 */
	public MetaDataParser(Connection con) throws SQLException {
		this.con = con;
		this.metadata = con.getMetaData();
		this.tableBase = new ArrayList<RDBMSTableBase>();
	}

	/**
	 * Parses the schema of a relational database to a relational database metadata representation
	 * @throws SQLException
	 */
	public void parse() throws SQLException {
		ResultSet rsTables = metadata.getTables(null, null, "%", new String[] { "TABLE" });

		while (rsTables.next()) {
			if (rsTables.getString("TABLE_TYPE").equals("TABLE")) {

				String tableName = rsTables.getString("TABLE_NAME");
				//used to store primary key metadata representation
				ArrayList<NameTypeTuple> primaryKeys = new ArrayList<NameTypeTuple>();
				//used to store foreign key metadata representation
				ArrayList<Tuple2<NameTypeTuple, String>> foreignKeys = new ArrayList<Tuple2<NameTypeTuple, String>>();
				//used to store further attributes metadata representation
				ArrayList<NameTypeTuple> furtherAttributes = new ArrayList<NameTypeTuple>();
				//used to find further attributes, respectively no primary or foreign key attributes 
				ArrayList<String> pkfkAttributes = new ArrayList<String>();

				ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
				ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);
				ResultSet rsAttributes = metadata.getColumns(null, null, tableName, null);

				//parses primary keys
				if (rsPrimaryKeys != null) {

					//assigning primary key name 
					while (rsPrimaryKeys.next()) {
						primaryKeys.add(new NameTypeTuple(rsPrimaryKeys.getString("COLUMN_NAME"), null));
						pkfkAttributes.add(rsPrimaryKeys.getString("COLUMN_NAME"));
					}
					
					//assigning primary key data type
					for (NameTypeTuple pk : primaryKeys) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, pk.f0);
						rsColumns.next();
						pk.f1 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				//parses foreign keys if exists
				if (rsForeignKeys != null) {

					//assigning foreign key name and name of belonging primary and foreign key table
					while (rsForeignKeys.next()) {
						foreignKeys.add(new Tuple2<NameTypeTuple,String>(new NameTypeTuple(rsForeignKeys.getString("FKCOLUMN_NAME"), null),
								rsForeignKeys.getString("PKTABLE_NAME")));
						pkfkAttributes.add(rsForeignKeys.getString("FKCOLUMN_NAME"));
					}
					
					//assigning foreign key data type
					for (Tuple2<NameTypeTuple, String> fk : foreignKeys) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, fk.f0.f0);
						rsColumns.next();
						fk.f0.f1 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				//parses further attributes if exists
				if (rsAttributes != null) {
					
					//assigning attribute name and belonging data type
					while (rsAttributes.next()) {
						if (!pkfkAttributes.contains(rsAttributes.getString("COLUMN_NAME"))) {
							furtherAttributes.add(new NameTypeTuple(rsAttributes.getString("COLUMN_NAME"),
									JDBCType.valueOf(rsAttributes.getInt("DATA_TYPE"))));
						}
					}
				}

				//number of rows (needed for distributed data querying via flink)
				int rowCount = TableRowSize.getTableRowSize(con, tableName);

				tableBase.add(new RDBMSTableBase(tableName, primaryKeys, foreignKeys, furtherAttributes, rowCount));
			}
		}
	}

	/**
	 * Creates metadata representations of tables, which will be convert to vertices
	 * 
	 * @return ArrayList containing metadata representations of rdbms tables going to convert to vertices
	 */
	public ArrayList<TableToNode> getTablesToNodes() {
		ArrayList<TableToNode> tablesToNodes = new ArrayList<TableToNode>();
		
		for (RDBMSTableBase tables : tableBase) {
			if (!(tables.getForeignKeys().size() == 2) || !(tables.getPrimaryKeys().size() == 2)) {
				tablesToNodes.add(new TableToNode(tables.getTableName(), tables.getPrimaryKeys(),
						tables.getForeignKeys(), tables.getFurtherAttributes(), tables.getRowCount()));
			}
		}
		return tablesToNodes;
	}

	/**
	 * Creates metadata representations of tables, which will be convert to edges
	 * 
	 * @return ArrayList containing metadata representations of rdbms tables going to convert to edges
	 */
	public ArrayList<TableToEdge> getTablesToEdges() {
		ArrayList<TableToEdge> tablesToEdges = new ArrayList<TableToEdge>();

		for (RDBMSTableBase table : tableBase) {
			if (table.getForeignKeys() != null) {
				int rowCount = table.getRowCount();
				
				//table tuples going to convert to edges
				if (table.getForeignKeys().size() == 2 && table.getPrimaryKeys().size() == 2) {
					tablesToEdges.add(new TableToEdge(table.getTableName(), table.getForeignKeys().get(0).f1, table.getForeignKeys().get(1).f1, table.getForeignKeys().get(0).f0,
							table.getForeignKeys().get(1).f0, null, table.getFurtherAttributes(), false, rowCount));
				} 
				
				//foreign keys going to convert to edges
				else {
					for (Tuple2<NameTypeTuple, String> fk : table.getForeignKeys()) {
						tablesToEdges.add(new TableToEdge(null, table.getTableName(), fk.f1, null, fk.f0, table.getPrimaryKeys(), null, true, rowCount));
					}
				}
			}
		}
		return tablesToEdges;
	}
}
