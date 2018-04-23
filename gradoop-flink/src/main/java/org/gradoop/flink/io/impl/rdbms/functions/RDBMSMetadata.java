package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

public class RDBMSMetadata {
	private DatabaseMetaData metadata;
	
	public RDBMSMetadata(Connection con) throws SQLException{
		this.metadata = con.getMetaData();
	}
	
	public DatabaseMetaData getMetadata(){
		return metadata;
	}
}
