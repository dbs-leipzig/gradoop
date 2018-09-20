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

/**
 * Stores database management system parameters
 */
public class RdbmsConfig {

  /**
   * Used database management system
   */
  private String rdbms;

  /**
   * Management system identifier
   */
  static public int RDBMSTYPE;

  /**
   * Valid jdbc url
   */
  private String url;

  /**
   * Username of database management system user
   */
  private String user;

  /**
   * Password of database management system user
   */
  private String pw;

  /**
   * Valid path to a fitting jdbc driver jar
   */
  private String jdbcDriverPath;

  /**
   * Valid and fitting jdbc driver class name
   */
  private String jdbcDriverClassName;

  /**
   * Constructor
   *
   * @param rdbms
   *          Database management system
   * @param url
   *          Valid jdbc url
   * @param user
   *          User name of database management system user
   * @param pw
   *          Password of database management system user
   * @param jdbcDriverPath
   *          Valid path to a fitting jdbc driver jar
   * @param jdbcDriverClassName
   *          Valid and fitting jdbc driver class name
   */
  public RdbmsConfig(String rdbms, String url, String user, String pw, String jdbcDriverPath,
      String jdbcDriverClassName) {
    this.rdbms = rdbms;
    this.url = url;
    this.user = user;
    this.pw = pw;
    this.jdbcDriverPath = jdbcDriverPath;
    this.jdbcDriverClassName = jdbcDriverClassName;
  }

  public int getRdbmsType() {
    return RDBMSTYPE;
  }

  public void setRdbmsType(int rdbmsType) {
    RdbmsConfig.RDBMSTYPE = rdbmsType;
  }

  public String getRdbms() {
    return rdbms;
  }

  /**
   * Assigns a rdbms type depending on connected management system.
   * @param rdbms Name of rdbms management system
   */
  public void setRdbms(String rdbms) {
    this.rdbms = rdbms;
    RdbmsConfig.RDBMSTYPE = RdbmsTypeChooser.choose(rdbms);
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPw() {
    return pw;
  }

  public void setPw(String pw) {
    this.pw = pw;
  }

  public String getJdbcDriverPath() {
    return jdbcDriverPath;
  }

  public void setJdbcDriverPath(String jdbcDriverPath) {
    this.jdbcDriverPath = jdbcDriverPath;
  }

  public String getJdbcDriverClassName() {
    return jdbcDriverClassName;
  }

  public void setJdbcDriverClassName(String jdbcDriverClassName) {
    this.jdbcDriverClassName = jdbcDriverClassName;
  }
}
