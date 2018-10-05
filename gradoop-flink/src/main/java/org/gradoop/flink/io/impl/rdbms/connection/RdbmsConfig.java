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

package org.gradoop.flink.io.impl.rdbms.connection;

import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants.RdbmsType;

/**
 * Stores database management system parameters
 */
public class RdbmsConfig {

  /**
   * Name of used relational database management system
   */
  private final String rdbmsName;

  /**
   * Management system identifier
   */
  private RdbmsType rdbmsType;

  /**
   * Valid jdbc url
   */
  private final String url;

  /**
   * Username of database management system user
   */
  private final String user;

  /**
   * Password of database management system user
   */
  private final String pw;

  /**
   * Valid path to a fitting jdbc driver jar
   */
  private final String jdbcDriverPath;

  /**
   * Valid and fitting jdbc driver class name
   */
  private final String jdbcDriverClassName;

  /**
   * Constructor
   *
   * @param rdbmsName Database management system
   * @param url Valid jdbc url
   * @param user User name of database management system user
   * @param pw Password of database management system user
   * @param jdbcDriverPath Valid path to a fitting jdbc driver jar
   * @param jdbcDriverClassName Valid and fitting jdbc driver class name
   */
  public RdbmsConfig(String rdbmsName, String url, String user, String pw, String jdbcDriverPath,
      String jdbcDriverClassName) {
    this.rdbmsName = rdbmsName;
    this.url = url;
    this.user = user;
    this.pw = pw;
    this.jdbcDriverPath = jdbcDriverPath;
    this.jdbcDriverClassName = jdbcDriverClassName;
  }

  /**
   * @return Relational database management system type.
   */
  public RdbmsType getRdbmsType() {
    return rdbmsType;
  }

  /**
   * @param rdbmsName Name of connected relational database management system.
   */
  public void setRdbmsType(String rdbmsName) {
    this.rdbmsType = RdbmsTypeChooser.choose(rdbmsName);
  }

  /**
   * @return Name if relational database management system.
   */
  public String getRdbmsName() {
    return rdbmsName;
  }

  /**
   * @return Relational database url.
   */
  public String getUrl() {
    return url;
  }

  /**
   * @return Relational database user name.
   */
  public String getUser() {
    return user;
  }

  /**
   * @return Relational database user password.
   */
  public String getPw() {
    return pw;
  }

  /**
   * @return Path of used jdbc driver.
   */
  public String getJdbcDriverPath() {
    return jdbcDriverPath;
  }

  /**
   * @return Used jdbc driver class name.
   */
  public String getJdbcDriverClassName() {
    return jdbcDriverClassName;
  }
}
