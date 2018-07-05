package org.gradoop.flink.io.impl.rdbms.connection;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Generic jdbc driver
 */
public class DriverShim implements Driver{
	private Driver driver;
	
	public DriverShim (Driver driver){
		this.driver = driver;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return this.driver.acceptsURL(url);
	}

	@Override
	public Connection connect(String url, Properties props) throws SQLException {
		return this.driver.connect(url, props);
	}

	@Override
	public int getMajorVersion() {
		return this.driver.getMajorVersion();
	}

	@Override
	public int getMinorVersion() {
		return this.driver.getMinorVersion();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return this.driver.getParentLogger();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException {
		return this.driver.getPropertyInfo(url, props);
	}

	@Override
	public boolean jdbcCompliant() {
		return this.driver.jdbcCompliant();
	}
}
