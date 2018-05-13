package org.gradoop.flink.io.impl.rdbms.sequential;

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class SequentialMetaDataParser {
	public SequentialMetaDataParser() {
	}

	public static ArrayList<RDBMSTable> parse(DatabaseMetaData metadata) throws Exception {
		ArrayList<RDBMSTable> tables = new ArrayList<RDBMSTable>();

		ResultSet rsTables = metadata.getTables(null, null, null, null);
		while (rsTables.next()) {
			String tableName = rsTables.getString("TABLE_NAME");
			ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
			ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);
			ResultSet rsAttributes = metadata.getColumns(null, null, tableName, "%");

			String pkString = "";
			while (rsPrimaryKeys.next()) {
				pkString = pkString + RDBMSConstants.PK_DELIMITER + rsPrimaryKeys.getString("COLUMN_NAME");
			}
			if (!pkString.isEmpty()) {
				if (!rsForeignKeys.next()) {
					RDBMSTable table = new RDBMSTable();
					table.setTableName(tableName);
					table.setPrimaryKey(pkString);
					table.setForeignKeys(null);
					LinkedHashMap<String, JDBCType> attr = new LinkedHashMap<String, JDBCType>();
					while (rsAttributes.next()) {
						attr.put(rsAttributes.getString("COLUMN_NAME"),
								JDBCType.valueOf(Integer.parseInt(rsAttributes.getString("DATA_TYPE"))));
					}
					table.setAttributes(attr);
					tables.add(table);
				} else {
					RDBMSTable table = new RDBMSTable();
					table.setTableName(tableName);
					table.setPrimaryKey(pkString);
					HashMap<String, String> fks = new HashMap<String, String>();
					do {
						fks.put(rsForeignKeys.getString("FKCOLUMN_NAME"), rsForeignKeys.getString("PKTABLE_NAME"));
					} while (rsForeignKeys.next());
					table.setForeignKeys(fks);
					LinkedHashMap<String, JDBCType> attr = new LinkedHashMap<String, JDBCType>();
					while (rsAttributes.next()) {
						attr.put(rsAttributes.getString("COLUMN_NAME"),
								JDBCType.valueOf(Integer.parseInt(rsAttributes.getString("DATA_TYPE"))));
					}
					table.setAttributes(attr);
					tables.add(table);
				}
			}
		}
		return tables;
	}
}
