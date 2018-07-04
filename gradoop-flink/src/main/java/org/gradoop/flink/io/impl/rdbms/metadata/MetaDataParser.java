package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.gradoop.flink.io.impl.rdbms.connection.RDBMSConnect;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.functions.TableRowSize;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TablesToEdges;
import org.gradoop.flink.io.impl.rdbms.tempGraphDSUsing.TablesToNodes;
import org.gradoop.flink.io.impl.rdbms.tuples.AttTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.FKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.PKTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

import scala.Array;

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
				ArrayList<NameTypeTuple> foreignKeys = new ArrayList<NameTypeTuple>();
				ArrayList<NameTypeTuple> furtherAttributes = new ArrayList<NameTypeTuple>();
				ArrayList<String> pkfkAttributes = new ArrayList<String>();

				int pos = 0;

				ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
				ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);
				ResultSet rsAttributes = metadata.getColumns(null, null, tableName, null);

				if (rsPrimaryKeys != null) {

					while (rsPrimaryKeys.next()) {
						primaryKeys.add(new PKTuple(rsPrimaryKeys.getString("COLUMN_NAME"), RDBMSConstants.PK_FIELD,
								pos, null));
						pkfkAttributes.add(rsPrimaryKeys.getString("COLUMN_NAME"));
						pos++;
					}
					for (PKTuple pk : primaryKeys) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, pk.f0);
						rsColumns.next();
						pk.f3 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				if (rsForeignKeys != null) {

					while (rsForeignKeys.next()) {
						foreignKeys.add(new FKTuple(tableName, rsForeignKeys.getString("PKTABLE_NAME"),
								rsForeignKeys.getString("FKCOLUMN_NAME"), rsForeignKeys.getString("PKCOLUMN_NAME"), 0,
								null));
						pkfkAttributes.add(rsForeignKeys.getString("FKCOLUMN_NAME"));
						pos++;
					}
					for (FKTuple fk : foreignKeys) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, fk.f2);
						rsColumns.next();
						fk.f5 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				if (rsAttributes != null) {

					while (rsAttributes.next()) {
						if (!pkfkAttributes.contains(rsAttributes.getString("COLUMN_NAME"))) {
							furtherAttributes.add(
									new AttTuple(rsAttributes.getString("COLUMN_NAME"), RDBMSConstants.ATTRIBUTE_FIELD,
											pos, JDBCType.valueOf(rsAttributes.getInt("DATA_TYPE"))));
							pos++;
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
				tablesToNodes.add(new TableToNode(tables.getTableName(), tables.getPrimaryKeys(),tables.getForeignKeys(),
						tables.getFurtherAttributes(), tables.getRowCount()));
			}
		}
		return tablesToNodes;
	}

	public ArrayList<TablesToEdges> getTablesToEdges() {
		ArrayList<TablesToEdges> tablesToEdges = new ArrayList<TablesToEdges>();

		for (RDBMSTableBase table : tableBase) {
			if (table.getForeignKeys() != null) {
				int rowCount = table.getRowCount();
				if (table.getForeignKeys().size() == 2 && table.getPrimaryKeys().size() == 2) {
					String tableName = table.getTableName();
					String fk1Table = table.getForeignKeys().get(0).getRefdTable();
					String fk2Table = table.getForeignKeys().get(1).getRefdTable();
					NameTypeTuple fk1Att = new NameTypeTuple(table.getForeignKeys().get(0).getRefingAttribute(),table.getForeignKeys().get(0).f5);
					NameTypeTuple fk2Att = new NameTypeTuple(table.getForeignKeys().get(1).getRefingAttribute(),table.getForeignKeys().get(1).f5);
					tablesToEdges.add(new TablesToEdges(tableName, fk1Table, fk2Table, null, fk1Att, fk2Att, false,
							table.getFurtherAttributes(), rowCount));
				} else {
					for (FKTuple fk : table.getForeignKeys()) {
						String pkConcat = concatPK(table);
						tablesToEdges.add(new TablesToEdges(null, table.getTableName(), fk.getRefdTable(),table.getPrimaryKeys(),new NameTypeTuple(fk.getRefingAttribute(),fk.f5),
								new NameTypeTuple(fk.getRefdAttribute(),fk.f5), true, null, rowCount));
					}
				}
			}
		}
		return tablesToEdges;
	}

	public String concatPK(RDBMSTableBase table) {
		String pkConcat = "";
		if (table.getPrimaryKeys().size() > 1) {
			for (PKTuple pk : table.getPrimaryKeys()) {
				pkConcat += pk.getName() + RDBMSConstants.PK_DELIMITER;
			}
		} else {
			pkConcat = table.getPrimaryKeys().get(0).getName();
		}
		return pkConcat;
	}
	// public static void main(String[] args) throws SQLException {
	// RDBMSConfig rdbmsConfig = new
	// RDBMSConfig("jdbc:mariadb://localhost/employees_small", "hr73vexy",
	// "UrlaubsReisen",
	// "/home/hr73vexy/01
	// Uni/gradoop_RdbmsToGraph/jdbcDrivers/mariadb-java-client-2.2.5.jar!/");
	//
	// Connection con = RDBMSConnect.connect(rdbmsConfig);
	//
	// MetaDataParser mp = new MetaDataParser(con);
	//
	// mp.parse();
	//
	// System.out.println("To Nodes !");
	//
	// ArrayList<RDBMSTableBase> toNodesTables = mp.getToNodesTables();
	//
	// for (RDBMSTableBase table : toNodesTables) {
	// System.out.println(table.getTableName());
	// for (TableTuple tt : table.getPrimaryKeys()) {
	// System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
	// }
	// for (Entry<TableTuple, String> tt : table.getForeignKeys().entrySet()) {
	// System.out.println(tt.getKey().f0 + " " + tt.getKey().f1 + " " +
	// tt.getKey().f2 + " " + tt.getKey().f3
	// + " " + tt.getValue());
	// }
	// for (TableTuple tt : table.getFurtherAttributes()) {
	// System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
	// }
	// }
	//
	// System.out.println("To Edges !");
	//
	// ArrayList<RDBMSTableBase> toEdgesTables = mp.getToEdgesTables();
	//
	// for (RDBMSTableBase table : toEdgesTables) {
	// System.out.println(table.getTableName());
	// for (TableTuple tt : table.getPrimaryKeys()) {
	// System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
	// }
	// for (Entry<TableTuple, String> tt : table.getForeignKeys().entrySet()) {
	// System.out.println(tt.getKey().f0 + " " + tt.getKey().f1 + " " +
	// tt.getKey().f2 + " " + tt.getKey().f3
	// + " " + tt.getValue());
	// }
	// if (table.getFurtherAttributes() != null) {
	// for (TableTuple tt : table.getFurtherAttributes()) {
	// System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
	// }
	// }
	// }
	// }

	// /**
	// * Queries rdbms' metadata
	// * @param metadata
	// * @param con
	// * @return
	// * @throws Exception
	// */
	// public static ArrayList<RDBMSTable> parse(DatabaseMetaData metadata,
	// Connection con) throws Exception {
	// ArrayList<RDBMSTable> tables = new ArrayList<RDBMSTable>();
	// ResultSet rsTables = metadata.getTables(null, null, "%", new
	// String[]{"TABLE"});
	// ResultSet schema = metadata.getSchemas();
	//
	// /*
	// * iterate over all tables of the connected rdbms
	// */
	// while (rsTables.next()) {
	//
	// /*
	// * only tables going to convert (e.g. no views,...)
	// */
	// if (rsTables.getString("TABLE_TYPE").equals("TABLE")) {
	// String tableName = rsTables.getString("TABLE_NAME");
	//
	// /*
	// * create new rdbms table representation
	// */
	// RDBMSTable table = new RDBMSTable();
	// table.setTableName(tableName);
	//
	// /*
	// * storing primary keys of the table
	// */
	// ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
	//// HashSet<String> primaryKeySet = new HashSet<String>();
	// ArrayList<String> primaryKeys = new ArrayList<String>();
	// while (rsPrimaryKeys.next()) {
	//// primaryKeySet.add(rsPrimaryKeys.getString("COLUMN_NAME"));
	// primaryKeys.add(rsPrimaryKeys.getString("COLUMN_NAME"));
	// }
	//
	// /*
	// * storing foreignkeys of the table
	// */
	// ResultSet rsForeignKeys = metadata.getImportedKeys(null, null,
	// tableName);
	// LinkedHashMap<String, String> foreignKeys = new LinkedHashMap<String,
	// String>();
	// while (rsForeignKeys.next()) {
	// foreignKeys.put(rsForeignKeys.getString("FKCOLUMN_NAME"),
	// rsForeignKeys.getString("PKTABLE_NAME"));
	// }
	//
	//
	// /*
	// * storing every attributename in the table
	// */
	// ResultSet rsColumns = metadata.getColumns(null, null, tableName, null);
	// ArrayList<String> columns = new ArrayList<String>();
	// while (rsColumns.next()) {
	// String attName = rsColumns.getString("COLUMN_NAME");
	// columns.add(attName);
	// table.getAttributes().put(attName,
	// JDBCType.valueOf(Integer.parseInt(rsColumns.getString("DATA_TYPE"))));
	// }
	//
	// /*
	// * add primary and foreign key's position in the table (needed for
	// accessing
	// fields via JDBCInputFormat)
	// */
	// for (String cols : columns) {
	// if (primaryKeys.contains(cols)) {
	// table.getPrimaryKey().add(cols);
	// }
	// if (foreignKeys.containsKey(cols)) {
	// table.getForeignKeys().put(cols, foreignKeys.get(cols));
	// }
	// }
	//
	// /*
	// * set the number of table's rows (needed for distributing/ pageination in
	// queriing via JDBCInputFormat)
	// */
	// table.setNumberOfRows(TableRowSize.getTableRowSize(con, tableName));
	//
	// /*
	// * set edge direction
	// */
	// if(table.getForeignKeys().size() == 2 && table.getPrimaryKey().size() ==
	// 2){
	// // for n:m relations
	// table.setDirectionIndicator(false);
	// }else{
	// // for 1:1,1:n relations
	// table.setDirectionIndicator(true);
	// }
	//
	// tables.add(table);
	// }
	// }
	//
	// return tables;
	// }
}
