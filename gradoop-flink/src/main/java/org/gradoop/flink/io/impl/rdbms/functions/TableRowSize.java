package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Provides the number of rows of a relational database
 */
public class TableRowSize {
	
	/**
	 * Queries a relational database to get the number of rows 
	 * @param con Valid jdbc database connection
	 * @param tableName Name of database table
	 * @return Number of rows of database
	 * @throws SQLException
	 */
	public static int getTableRowSize(Connection con, String tableName) {
		int rowNumber = 0;
		Statement st;
		try {
			st = con.createStatement();
		
		ResultSet rs = st.executeQuery("select count(*) from " + tableName);
		if (rs.next()) {
			rowNumber = rs.getInt(1);
		} else {
			rowNumber = 0;
		}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rowNumber;
	}
}
