package org.gradoop.flink.io.impl.rdbms.sequential;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

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
					tables.add(table);
				}
			}
		}
		return tables;
	}
}
