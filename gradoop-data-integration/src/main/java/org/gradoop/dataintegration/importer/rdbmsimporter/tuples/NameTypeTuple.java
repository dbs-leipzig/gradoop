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
package org.gradoop.dataintegration.importer.rdbmsimporter.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.JDBCType;

/**
 * Represents a pair of [attribute name, JDBC data type]
 * f0 : attribute name
 * f1 : JDBC data type
 */
public class NameTypeTuple extends Tuple2<String, JDBCType> {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Empty Constructor.
   */
  public NameTypeTuple() { }

  /**
   * Creates an instance of {@link NameTypeTuple} to store attribute name and belonging JDBC data
   * type.
   *
   * @param attributeName name of attribute
   * @param jdbcType JDBC type of attribute
   */
  public NameTypeTuple(String attributeName, JDBCType jdbcType) {
    super(attributeName, jdbcType);
  }

  /**
   * Get serial version uid.
   * @return serial version uid
   */
  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  /**
   * Get attribute name.
   * @return attribute name
   */
  public String getAttributeName() {
    return this.f0;
  }

  /**
   * get JDBC type of attribute.
   * @return JDBC type
   */
  public JDBCType getJdbcType() {
    return this.f1;
  }

  /**
   * Set name of attribute.
   * @param attributeName attribute name
   */
  public void setName(String attributeName) {
    this.f0 = attributeName;
  }

  /**
   * Set JDBC Type of attribute.
   * @param jdbcType JDBC type
   */
  public void setType(JDBCType jdbcType) {
    this.f1 = jdbcType;
  }
}
