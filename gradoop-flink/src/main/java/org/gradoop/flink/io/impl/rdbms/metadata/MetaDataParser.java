package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

import org.gradoop.flink.io.impl.rdbms.functions.TableRowSize;

/**
 * Determines rdbms' metadata 
 * @author pc
 *
 */
public class MetaDataParser {

	/**
	 * Queries rdbms' metadata 
	 * @param metadata
	 * @param con
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<RDBMSTable> parse(DatabaseMetaData metadata, Connection con) throws Exception {
		ArrayList<RDBMSTable> tables = new ArrayList<RDBMSTable>();
		ResultSet rsTables = metadata.getTables(null, null, "%", new String[]{"TABLE"});
		ResultSet schema = metadata.getSchemas();
		
		/*
		 * iterate over all tables of the connected rdbms
		 */
		while (rsTables.next()) {
			
			/*
			 * only tables going to convert (e.g. no views,...)
			 */
			if (rsTables.getString("TABLE_TYPE").equals("TABLE")) {
				String tableName = rsTables.getString("TABLE_NAME");
				
				/*
				 * create new rdbms table representation
				 */
				RDBMSTable table = new RDBMSTable();
				table.setTableName(tableName);

				/*
				 * storing primary keys of the table
				 */
				ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
//				HashSet<String> primaryKeySet = new HashSet<String>();
				ArrayList<String> primaryKeys = new ArrayList<String>();
				while (rsPrimaryKeys.next()) {
//					primaryKeySet.add(rsPrimaryKeys.getString("COLUMN_NAME"));
					primaryKeys.add(rsPrimaryKeys.getString("COLUMN_NAME"));
				}

				/*
				 * storing foreignkeys of the table 
				 */
				ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);
				LinkedHashMap<String, String> foreignKeys = new LinkedHashMap<String, String>();
				while (rsForeignKeys.next()) {
					foreignKeys.put(rsForeignKeys.getString("FKCOLUMN_NAME"),
							rsForeignKeys.getString("PKTABLE_NAME"));
				}
				

				/*
				 * storing every attributename in the table 
				 */
				ResultSet rsColumns = metadata.getColumns(null, null, tableName, null);
				ArrayList<String> columns = new ArrayList<String>();
				while (rsColumns.next()) {
					String attName = rsColumns.getString("COLUMN_NAME");
					columns.add(attName);
					table.getAttributes().put(attName,
							JDBCType.valueOf(Integer.parseInt(rsColumns.getString("DATA_TYPE"))));
				}

				/*
				 * add primary and foreign key's position in the table (needed for accessing fields via JDBCInputFormat)
				 */
				for (String cols : columns) {
					if (primaryKeys.contains(cols)) {
						table.getPrimaryKey().add(cols);
					}
					if (foreignKeys.containsKey(cols)) {
						table.getForeignKeys().put(cols, foreignKeys.get(cols));
					}
				}
				
				/*
				 * set the number of table's rows (needed for distributing/ pageination in queriing via JDBCInputFormat)
				 */
				table.setNumberOfRows(TableRowSize.getTableRowSize(con, tableName));
				
				/*
				 * set edge direction
				 */
				if(table.getForeignKeys().size() == 2 && table.getPrimaryKey().size() == 2){
					// for n:m relations
					table.setDirectionIndicator(false);
				}else{
					// for 1:1,1:n relations
					table.setDirectionIndicator(true);
				}
				
				tables.add(table);
			}
		}
		
		return tables;
	}
}
