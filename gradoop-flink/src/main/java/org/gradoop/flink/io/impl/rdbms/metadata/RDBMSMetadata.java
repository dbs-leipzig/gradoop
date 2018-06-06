package org.gradoop.flink.io.impl.rdbms.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * class providing rdbms metadata
 * @author pc
 * @return rdbms metadata
 */
public class RDBMSMetadata {

	public RDBMSMetadata() {
	}

	public static DatabaseMetaData getDBMetaData(Connection connection) {
		DatabaseMetaData dbmetadata = null;
		try {
			dbmetadata = connection.getMetaData();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println("Not possible to get database metadata!");
			e.printStackTrace();
		}
		return dbmetadata;
	}
}
