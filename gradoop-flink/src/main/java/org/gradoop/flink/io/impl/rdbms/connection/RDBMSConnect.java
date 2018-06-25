package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSConfig;

/**
 * Class for connecting to a rdbms via jdbc
 * 
 * @author pc
 * @return valid database connection
 */

public class RDBMSConnect {
	public RDBMSConnect() {
	}

	public static Connection connect(RDBMSConfig config) {
		Connection connection = null;
		
		try {
			new RegisterDriver().register(config);
			connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPw());
			System.out.println("***Successfully Connected to DB : " + config.getUrl().replaceAll(".*/", ""));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println("Not possible to establish database connection to DBUrl " + config.getUrl() + " !");
			e.printStackTrace();
		}
		return connection;
	}
}
