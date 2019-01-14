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

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Represents a row header consists of attribute name, attribute role (primary-, foreign key,
 * further attribute) and position in row (starts from left first position with 0).
 * f0 : attribute name
 * f1 : attribute role
 * f2 : attribute`s row position
 */
public class RowHeaderTuple extends Tuple3<String, String, Integer> {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Empty Constructor
   */
  public RowHeaderTuple() { }

  /**
   * Creates an instance of {@link RowHeaderTuple} to locate and assign values in row format.
   *
   * @param attributeName name of attribute
   * @param attributeRole role of attribute
   * @param rowPosition row position of attribute
   */
  public RowHeaderTuple(String attributeName, String attributeRole, int rowPosition) {
    super(attributeName, attributeRole, rowPosition);
  }

  /**
   * Get serial version uid.
   *
   * @return serial version uid
   */
  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  /**
   * Get name of attribute.
   *
   * @return attribute name
   */
  public String getAttributeName() {
    return this.f0;
  }

  /**
   * Get attribute role.
   *
   * @return attribute role
   */
  public String getAttributeRole() {
    return this.f1;
  }

  /**
   * Get row position of attribute.
   *
   * @return row position
   */
  public int getRowPostition() {
    return this.f2;
  }

  /**
   * Set attribute name.
   *
   * @param attributeName attribute name
   */
  public void setAttributeName(String attributeName) {
    this.f0 = attributeName;
  }

  /**
   * Set role of attribute.
   *
   * @param attributeRole attribute role
   */
  public void setAttributeRole(String attributeRole) {
    this.f1 = attributeRole;
  }

  /**
   * Set row position of attribute.
   *
   * @param rowPosition row position
   */
  public void rowPosition(int rowPosition) {
    this.f2 = rowPosition;
  }
}
