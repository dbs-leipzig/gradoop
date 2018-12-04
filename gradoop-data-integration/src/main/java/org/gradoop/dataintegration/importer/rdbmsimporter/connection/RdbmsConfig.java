/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.RdbmsType;

/**
 * Stores database management system parameters
 */
public class  RdbmsConfig {

  /**
   * Name of database instance.
   */
  private final String rdbmsName;

  /**
   * Type of database management system.
   */
  private RdbmsType rdbmsType;

  /**
   * JDBC database url.
   */
  private final String url;

  /**
   * Name of database user.
   */
  private final String user;

  /**
   * Password of database user.
   */
  private final String pw;

  /**
   * Path to JDBC driver .jar file.
   */
  private final String jdbcDriverPath;

  /**
   * JDBC driver class name.
   */
  private final String jdbcDriverClassName;

  /**
   * Creates an instance of {@link RdbmsConfig} to store the configuration of a database instance.
   *
   * @param rdbmsName database management system
   * @param url valid JDBC url
   * @param user user name of database user
   * @param pw password of database user
   * @param jdbcDriverPath valid path to a proper JDBC driver .jar file
   * @param jdbcDriverClassName valid and proper JDBC driver class name
   */
  public RdbmsConfig(String rdbmsName,
    String url,
    String user,
    String pw,
    String jdbcDriverPath,
    String jdbcDriverClassName) {
    this.rdbmsName = rdbmsName;
    this.url = url;
    this.user = user;
    this.pw = pw;
    this.jdbcDriverPath = jdbcDriverPath;
    this.jdbcDriverClassName = jdbcDriverClassName;
  }

  /**
   * Get type of connected database.
   *
   * @return database management system type
   */
  public RdbmsType getRdbmsType() {
    return rdbmsType;
  }

  /**
   * Set type of connected database.
   *
   * @param rdbmsName name of connected database instance
   */
  public void setRdbmsType(String rdbmsName) {
    this.rdbmsType = RdbmsTypeChooser.create().choose(rdbmsName);
  }

  /**
   * Get name of database instance.
   *
   * @return name of database instance
   */
  public String getRdbmsName() {
    return rdbmsName;
  }

  /**
   * Get JDBC url of database connection.
   *
   * @return JDBC url
   */
  public String getUrl() {
    return url;
  }

  /**
   * Get user name.
   *
   * @return user name
   */
  public String getUser() {
    return user;
  }

  /**
   * Get user password.
   *
   * @return user password
   */
  public String getPw() {
    return pw;
  }

  /**
   * Get path to JDBC driver .jar file
   *
   * @return path to JDBC driver
   */
  public String getJdbcDriverPath() {
    return jdbcDriverPath;
  }

  /**
   * Get JDBC driver class name
   *
   * @return JDBC driver class name
   */
  public String getJdbcDriverClassName() {
    return jdbcDriverClassName;
  }
}
