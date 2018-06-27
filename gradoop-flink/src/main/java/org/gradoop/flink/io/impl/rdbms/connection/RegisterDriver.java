package org.gradoop.flink.io.impl.rdbms.connection;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSConfig;

public class RegisterDriver {

	public static void register(RDBMSConfig config) {
		String driverClassName = getDriverClassName(config.getUrl());
		
		try {
			URL driverUrl = new URL("jar:file:" + config.getJdbcDriverPath());
			URLClassLoader ucl = new URLClassLoader(new URL[] { driverUrl });
			Driver driver = (Driver) Class.forName(driverClassName, true, ucl).newInstance();
			DriverManager.registerDriver(new DriverShim(driver));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.err.println("Not possible to establish database connection to DBUrl " + config.getUrl() + " !");
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getDriverClassName(String url) {
		return DriverClassNameChooser.choose(url);
	}
}
