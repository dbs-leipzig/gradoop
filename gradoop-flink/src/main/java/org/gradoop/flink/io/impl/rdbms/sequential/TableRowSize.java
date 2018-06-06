package org.gradoop.flink.io.impl.rdbms.sequential;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

public class TableRowSize {
	int tableRowSize;

	public TableRowSize() {
	}

	public static int getTableRowSize(Connection con, String tableName) throws SQLException {
		int rowNumber;
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("select count(*) from " + tableName);
		if (rs.next()) {
			rowNumber = rs.getInt(1);
		} else {
			rowNumber = 0;
		}
		return rowNumber;
	}
}
