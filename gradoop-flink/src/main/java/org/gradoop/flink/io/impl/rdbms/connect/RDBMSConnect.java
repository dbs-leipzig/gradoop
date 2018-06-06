package org.gradoop.flink.io.impl.rdbms.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Class for connecting to a rdbms via jdbc
 * @author pc
 * @return valid database connection
 */

public class RDBMSConnect {
	public RDBMSConnect() {
	}

	public static Connection connect(RDBMSConfig config) {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPw());
			System.out.println("Connection to DB successfully!");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println("Not possible to establish database connection!");
			e.printStackTrace();
		}
		return connection;
	}
}
