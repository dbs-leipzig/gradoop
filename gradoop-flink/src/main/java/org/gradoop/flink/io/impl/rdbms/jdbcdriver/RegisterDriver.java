package org.gradoop.flink.io.impl.rdbms.jdbcdriver;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.gradoop.flink.io.impl.rdbms.connect.DriverClassNameChooser;
import org.gradoop.flink.io.impl.rdbms.connect.DriverFileNameChooser;
import org.gradoop.flink.io.impl.rdbms.connect.RDBMSConfig;
import org.gradoop.flink.io.impl.rdbms.constants.RDBMSConstants;

public class RegisterDriver {

	public static void register(RDBMSConfig config) {
		String driverFileName = getDriverFileName(config.getUrl());
		String driverClassName = getDriverClassName(config.getUrl());
		try {
			URL driverUrl = new URL("jar:file:" + getDriverJarPath() + RDBMSConstants.JARS_PATH + driverFileName);
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

	public static String getDriverJarPath() {
		return new File(System.getProperty("user.dir")).getAbsolutePath().replaceAll("/gradoop-examples|/gradoop-flink",
				"");
	}

	public static String getDriverFileName(String url) {
		return new DriverFileNameChooser().choose(url);
	}

	public static String getDriverClassName(String url) {
		return new DriverClassNameChooser().choose(url);
	}
}
