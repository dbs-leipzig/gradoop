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
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.tuples.TableTuple;

/**
 * Determines rdbms' metadata
 * 
 * @author pc
 *
 */
public class MetaDataParser {

	private Connection con;
	private DatabaseMetaData metadata;
	private ArrayList<RDBMSTableBase> toNodesTables;
	private ArrayList<RDBMSTableBase> toEdgesTables;

	public MetaDataParser(Connection con) {
		this.con = con;
		try {
			this.metadata = con.getMetaData();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Can't get Database Metadata !");
		}
		this.toNodesTables = new ArrayList<RDBMSTableBase>();
		this.toEdgesTables = new ArrayList<RDBMSTableBase>();
	}

	public void parse() throws SQLException {

		ResultSet rsTables = metadata.getTables(null, null, "%", new String[] { "TABLE" });

		while (rsTables.next()) {

			/*
			 * only tables going to convert (e.g. no views,...)
			 */
			if (rsTables.getString("TABLE_TYPE").equals("TABLE")) {

				String tableName = rsTables.getString("TABLE_NAME");
				ArrayList<TableTuple> primaryKeys = new ArrayList<TableTuple>();
				LinkedHashMap<TableTuple, String> foreignKeys = new LinkedHashMap<TableTuple, String>();
				ArrayList<TableTuple> furtherAttributes = new ArrayList<TableTuple>();
				ArrayList<String> pkfkAttributes = new ArrayList<String>();

				int pos = 0;

				ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
				ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);
				ResultSet rsAttributes = metadata.getColumns(null, null, tableName, null);

				if (rsPrimaryKeys != null) {

					while (rsPrimaryKeys.next()) {
						primaryKeys.add(new TableTuple(rsPrimaryKeys.getString("COLUMN_NAME"), RDBMSConstants.PK_FIELD,
								pos, null));
						pkfkAttributes.add(rsPrimaryKeys.getString("COLUMN_NAME"));
						pos++;
					}
					for (TableTuple pk : primaryKeys) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, pk.f0);
						rsColumns.next();
						pk.f3 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				if (rsForeignKeys != null) {

					while (rsForeignKeys.next()) {
						foreignKeys.put(new TableTuple(rsForeignKeys.getString("FKCOLUMN_NAME"),
								RDBMSConstants.FK_FIELD, pos, null), rsForeignKeys.getString("PKTABLE_NAME"));
						pkfkAttributes.add(rsForeignKeys.getString("COLUMN_NAME"));
						pos++;
					}
					for (Entry<TableTuple, String> fk : foreignKeys.entrySet()) {
						ResultSet rsColumns = metadata.getColumns(null, null, tableName, fk.getKey().f0);
						rsColumns.next();
						fk.getKey().f3 = JDBCType.valueOf(rsColumns.getInt("DATA_TYPE"));
					}
				}

				if (rsAttributes != null) {

					while (rsAttributes.next()) {
						if (!pkfkAttributes.contains(rsAttributes.getString("COLUMN_NAME"))) {
							furtherAttributes.add(new TableTuple(rsAttributes.getString("COLUMN_NAME"),
									RDBMSConstants.ATTRIBUTE_FIELD, pos,
									JDBCType.valueOf(rsAttributes.getInt("DATA_TYPE"))));
							pos++;
						}
					}
				}

				int rowCount = TableRowSize.getTableRowSize(con, tableName);

				if (primaryKeys.size() == 2 && foreignKeys.size() == 2) {
					toEdgesTables.add(new RDBMSTableBase(tableName, false, primaryKeys, foreignKeys, furtherAttributes,
							false, rowCount));
				} else {
					toNodesTables.add(new RDBMSTableBase(tableName, true, primaryKeys, foreignKeys, furtherAttributes,
							true, rowCount));
					if (!foreignKeys.isEmpty()) {
						toEdgesTables
								.add(new RDBMSTableBase(tableName, false, (ArrayList<TableTuple>) primaryKeys.clone(),
										(LinkedHashMap<TableTuple, String>) foreignKeys.clone(), null, true, rowCount));
					}
				}
			}
		}
	}

	public ArrayList<RDBMSTableBase> getToNodesTables() throws SQLException {
		return toNodesTables;
	}

	public ArrayList<RDBMSTableBase> getToEdgesTables() {
		return toEdgesTables;
	}

	public static void main(String[] args) throws SQLException {
		RDBMSConfig rdbmsConfig = new RDBMSConfig("jdbc:mariadb://localhost/employees_small", "hr73vexy",
				"UrlaubsReisen",
				"/home/hr73vexy/01 Uni/gradoop_RdbmsToGraph/jdbcDrivers/mariadb-java-client-2.2.5.jar!/");

		Connection con = RDBMSConnect.connect(rdbmsConfig);

		MetaDataParser mp = new MetaDataParser(con);

		mp.parse();

		System.out.println("To Nodes !");

		ArrayList<RDBMSTableBase> toNodesTables = mp.getToNodesTables();

		for (RDBMSTableBase table : toNodesTables) {
			System.out.println(table.getTableName());
			for (TableTuple tt : table.getPrimaryKeys()) {
				System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
			}
			for (Entry<TableTuple, String> tt : table.getForeignKeys().entrySet()) {
				System.out.println(tt.getKey().f0 + " " + tt.getKey().f1 + " " + tt.getKey().f2 + " " + tt.getKey().f3
						+ " " + tt.getValue());
			}
			for (TableTuple tt : table.getFurtherAttributes()) {
				System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
			}
		}

		System.out.println("To Edges !");

		ArrayList<RDBMSTableBase> toEdgesTables = mp.getToEdgesTables();

		for (RDBMSTableBase table : toEdgesTables) {
			System.out.println(table.getTableName());
			for (TableTuple tt : table.getPrimaryKeys()) {
				System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
			}
			for (Entry<TableTuple, String> tt : table.getForeignKeys().entrySet()) {
				System.out.println(tt.getKey().f0 + " " + tt.getKey().f1 + " " + tt.getKey().f2 + " " + tt.getKey().f3
						+ " " + tt.getValue());
			}
			if (table.getFurtherAttributes() != null) {
				for (TableTuple tt : table.getFurtherAttributes()) {
					System.out.println(tt.f0 + " " + tt.f1 + " " + tt.f2 + " " + tt.f3);
				}
			}
		}
	}

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
	// ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);
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
	// * add primary and foreign key's position in the table (needed for accessing
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
	// if(table.getForeignKeys().size() == 2 && table.getPrimaryKey().size() == 2){
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
