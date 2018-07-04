package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.rdbms.functions.TableRowSize;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;

/**
 * Determines rdbms' metadata
 * 
 * @author pc
 *
 */
public class MetaDataParser {
	private Connection con;
	private DatabaseMetaData metadata;
	private ArrayList<RDBMSTableBase> tableBase;

	public MetaDataParser(Connection con) throws SQLException {
		this.con = con;
		this.metadata = con.getMetaData();
		this.tableBase = new ArrayList<RDBMSTableBase>();
	}

	public void parse() throws SQLException {
		ResultSet rsTables = metadata.getTables(null, null, "%", new String[] { "TABLE" });

		while (rsTables.next()) {

			/*
			 * only tables going to convert (e.g. no views,...)
			 */
			if (rsTables.getString("TABLE_TYPE").equals("TABLE")) {

				String tableName = rsTables.getString("TABLE_NAME");
				ArrayList<NameTypeTuple> primaryKeys = new ArrayList<NameTypeTuple>();
				ArrayList<Tuple2<NameTypeTuple, String>> foreignKeys = new ArrayList<Tuple2<NameTypeTuple, String>>();
				ArrayList<NameTypeTuple> furtherAttributes = new ArrayList<NameTypeTuple>();
				ArrayList<String> pkfkAttributes = new ArrayList<String>();

				ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
				ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);
				ResultSet rsAttributes = metadata.getColumns(null, null, tableName, null);

				if (rsPrimaryKeys != null) {

					while (rsPrimaryKeys.next()) {
						primaryKeys.add(new NameTypeTuple(rsPrimaryKeys.getString("COLUMN_NAME"), null));
						pkfkAttributes.add(rsPrimaryKeys.getString("COLUMN_NAME"));
					}
					for (NameTypeTuple pk : primaryKeys) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, pk.f0);
						rsColumns.next();
						pk.f1 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				if (rsForeignKeys != null) {

					while (rsForeignKeys.next()) {
						foreignKeys.add(new Tuple2(new NameTypeTuple(rsForeignKeys.getString("FKCOLUMN_NAME"), null),
								rsForeignKeys.getString("PKTABLE_NAME")));
						pkfkAttributes.add(rsForeignKeys.getString("FKCOLUMN_NAME"));
					}
					for (Tuple2<NameTypeTuple, String> fk : foreignKeys) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, fk.f0.f0);
						rsColumns.next();
						fk.f0.f1 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				if (rsAttributes != null) {

					while (rsAttributes.next()) {
						if (!pkfkAttributes.contains(rsAttributes.getString("COLUMN_NAME"))) {
							furtherAttributes.add(new NameTypeTuple(rsAttributes.getString("COLUMN_NAME"),
									JDBCType.valueOf(rsAttributes.getInt("DATA_TYPE"))));
						}
					}
				}

				int rowCount = TableRowSize.getTableRowSize(con, tableName);

				tableBase.add(new RDBMSTableBase(tableName, primaryKeys, foreignKeys, furtherAttributes, rowCount));
			}
		}
	}

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

	public ArrayList<TableToEdge> getTablesToEdges() {
		ArrayList<TableToEdge> tablesToEdges = new ArrayList<TableToEdge>();

		for (RDBMSTableBase table : tableBase) {
			if (table.getForeignKeys() != null) {
				int rowCount = table.getRowCount();
				if (table.getForeignKeys().size() == 2 && table.getPrimaryKeys().size() == 2) {
					tablesToEdges.add(new TableToEdge(table.getTableName(), table.getForeignKeys().get(0).f1, table.getForeignKeys().get(1).f1, table.getForeignKeys().get(0).f0,
							table.getForeignKeys().get(1).f0, null, table.getFurtherAttributes(), false, rowCount));
				} else {
					for (Tuple2<NameTypeTuple, String> fk : table.getForeignKeys()) {
						tablesToEdges.add(new TableToEdge(null, table.getTableName(), fk.f1, null, fk.f0, table.getPrimaryKeys(), null, true, rowCount));
					}
				}
			}
		}
		return tablesToEdges;
	}
}
