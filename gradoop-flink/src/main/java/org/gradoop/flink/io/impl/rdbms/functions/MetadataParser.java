package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class MetadataParser {

	public MetadataParser() {
	}

	public static DataSet<Tuple4<String, String, String, String>> parse(DatabaseMetaData metadata,
			GradoopFlinkConfig config) throws Exception {
		ArrayList<Tuple4<String, String, String, String>> tables = new ArrayList<Tuple4<String, String, String, String>>();
		try {
			ResultSet rsTables = metadata.getTables(null, null, null, null);
			while (rsTables.next()) {
				String tableName = rsTables.getString("TABLE_NAME");
				ResultSet rsPrimaryKeys = metadata.getPrimaryKeys(null, null, tableName);
				ResultSet rsForeignKeys = metadata.getImportedKeys(null, null, tableName);

				String pkString = "";
				while (rsPrimaryKeys.next()) {
					pkString = pkString + RDBMSConstants.PK_DELIMITER + rsPrimaryKeys.getString("COLUMN_NAME");
				}

//				if (!pkString.isEmpty()) {
//					if (!rsForeignKeys.next()) {
//						Tuple4<String, String, String, String> table = new RDBMSTable();
//						table.f0 = tableName;
//						table.f1 = pkString;
//						table.f2 = "";
//						table.f3 = "";
//						tables.add(table);
//					} else {
//						do {
//							Tuple4<String, String, String, String> table = new RDBMSTable();
//							table.f0 = tableName;
//							table.f1 = pkString;
//							table.f2 = rsForeignKeys.getString("FKCOLUMN_NAME");
//							table.f3 = rsForeignKeys.getString("PKTABLE_NAME");
//							tables.add(table);
//						} while (rsForeignKeys.next());
//					}
//				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return config.getExecutionEnvironment().fromCollection(tables);
	}

}
