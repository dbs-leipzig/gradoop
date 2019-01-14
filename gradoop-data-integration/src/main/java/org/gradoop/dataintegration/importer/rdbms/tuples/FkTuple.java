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
package org.gradoop.dataintegration.importer.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

import java.sql.JDBCType;

/**
 * Represents a foreign key
 * f0 : attribute name (key) of referencing foreign key
 * f1 : JDBC type of referencing foreign key value
 * f2 : name of referenced primary key attribute
 * f3 : name of referenced table
 */
public class FkTuple extends Tuple4<String, JDBCType, String, String> {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Empty Constructor.
   */
  public FkTuple() {
  }

  /**
   * Creates an instance of {@link FkTuple} to store foreign key relevant information.
   *
   * @param foreignKeyName foreign key attribute name
   * @param jdbcType SQL type of foreign key values
   * @param referencedAttributeName name of referenced primary key
   * @param referencedTablename name of referenced table
   */
  public FkTuple(
    String foreignKeyName, JDBCType jdbcType, String referencedAttributeName,
    String referencedTablename) {

    super(foreignKeyName, jdbcType, referencedAttributeName, referencedTablename);
  }

  /**
   * Get serial version uid.
   * @return serial version uid
   */
  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  /**
   * Get name of foreign key attribute.
   * @return foreign key attribute name
   */
  public String getForeignKeyName() {
    return this.f0;
  }

  /**
   * Get JDBC type of foreign key values.
   * @return JDBC type of foreign key values
   */
  public JDBCType getJdbcType() {
    return this.f1;
  }

  /**
   * Get name of referenced primary key attribute.
   * @return referenced primary key attribute
   */
  public String getReferencedAttributeName() {
    return this.f2;
  }

  /**
   * Get name of referenced table.
   * @return name of referenced table
   */
  public String getReferencedTablename() {
    return this.f3;
  }

  /**
   * Set name of foreign key attribute.
   * @param foreignKeyName foreign key attribute name
   */
  public void setForeignKeyName(String foreignKeyName) {
    this.f0 = foreignKeyName;
  }

  /**
   * Set JDBC type of foreign key values.
   * @param jdbcType JDBC type of foreign key values
   */
  public void setJdbcType(JDBCType jdbcType) {
    this.f1 = jdbcType;
  }

  /**
   * Set name of referenced primary key attribute.
   * @param referencedAttributeName referenced primary key attribute.
   */
  public void setReferencedAttributeName(String referencedAttributeName) {
    this.f2 = referencedAttributeName;
  }

  /**
   * Set name of referenced table.
   * @param referencedTablename name of referenced table
   */
  public void setReferencedTablename(String referencedTablename) {
    this.f3 = referencedTablename;
  }
}
