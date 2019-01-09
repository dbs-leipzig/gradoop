/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.dataintegration.importer.rdbmsimporter.connection;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Implements a JDBC driver. Needed for registering a generic JDBC driver in
 * class loader.
 */
public class GradoopJDBCDriver implements Driver {

  /**
   * Valid java sql driver
   */
  private Driver driver;

  /**
   * Constructs a new wrapper around a Driver.
   *
   * @param driver Database driver to wrap
   */
  GradoopJDBCDriver(Driver driver) {
    this.driver = driver;
  }

  /**
   * Wraps the underlying driver's call to acceptsURL. Returns whether or not the
   * driver can open a connection to the given URL.
   *
   * @param url the URL of the database
   * @return true if the wrapped driver can connect to the specified URL
   * @throws SQLException thrown if there is an error connecting to the database
   */
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return this.driver.acceptsURL(url);
  }

  /**
   * Wraps the call to the underlying driver's connect method.
   *
   * @param url the URL of the database
   * @param props a collection of string/value pairs
   * @return a Connection object
   * @throws SQLException thrown if there is an error connecting to the database
   */
  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    return this.driver.connect(url, props);
  }

  /**
   * Returns the wrapped driver's major version number.
   *
   * @return the wrapped driver's major version number
   */
  @Override
  public int getMajorVersion() {
    return this.driver.getMajorVersion();
  }

  /**
   * Returns the wrapped driver's minor version number.
   *
   * @return the wrapped driver's minor version number
   */
  @Override
  public int getMinorVersion() {
    return this.driver.getMinorVersion();
  }

  /**
   * Wraps the call to the underlying driver's getParentLogger method.
   *
   * @return the parent's Logger
   * @throws SQLFeatureNotSupportedException thrown if the feature is not
   *           supported
   */
  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return this.driver.getParentLogger();
  }

  /**
   * Wraps the call to the underlying driver's getPropertyInfo method.
   *
   * @param url the URL of the database
   * @param props a collection of string/value pairs
   * @return an array of DriverPropertyInfo objects
   * @throws SQLException thrown if there is an error accessing the database
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException {
    return this.driver.getPropertyInfo(url, props);
  }

  /**
   * Returns whether or not the wrapped driver is jdbcCompliant.
   *
   * @return true if the wrapped driver is JDBC compliant; otherwise false
   */
  @Override
  public boolean jdbcCompliant() {
    return this.driver.jdbcCompliant();
  }
}
