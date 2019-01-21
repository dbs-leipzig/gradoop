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
package org.gradoop.dataintegration.importer.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Tuple for table to vertex (n:m relation) conversion
 * f0 : Foreign key one
 * f1 : Foreign key two
 * f2 : Properties
 */
public class Fk1Fk2Props extends Tuple3<String, String, Properties> {

  /**
   * Default serial version uid.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Empty Constructor.
   */
  public Fk1Fk2Props() { }

  /**
   * Creates an instance of {@link Fk1Fk2Props} to store two foreign key values and belonging
   * properties.
   *
   * @param foreignKeyOne Name of foreign key one
   * @param foreignKeyTwo Name of foreign key two
   * @param properties Relation belonging properties
   */
  public Fk1Fk2Props(String foreignKeyOne, String foreignKeyTwo, Properties properties) {

    super(foreignKeyOne, foreignKeyTwo, properties);
  }

  /**
   * Get serial versoin uid.
   * @return serial version uid
   */
  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  /**
   * Get value of foreign key one.
   * @return foreign key one value
   */
  public String getForeignKeyOne() {
    return this.f0;
  }

  /**
   * Get value of foreign key two.
   * @return foreign key two
   */
  public String getForeignKeyTwo() {
    return this.f1;
  }

  /**
   * Get properties.
   * @return properties
   */
  public Properties getProperties() {
    return this.f2;
  }

  /**
   * Set foreign key one.
   * @param foreignKeyOne foreign key one
   */
  public void setForeignKeyOne(String foreignKeyOne) {
    this.f0 = foreignKeyOne;
  }

  /**
   * Set foreign key two.
   * @param foreignKeyTwo foreign key two.
   */
  public void setForeignKeyTwo(String foreignKeyTwo) {
    this.f1 = foreignKeyTwo;
  }

  /**
   * Set properties.
   * @param properties properties
   */
  public void setProperties(Properties properties) {
    this.f2 = properties;
  }
}
