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
package org.gradoop.dataintegration.importer.rdbmsimporter.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;

/**
 * Tuple for n:m relation conversion f0 : Foreign key one f1 : Foreign key two f2 : Properties of
 * belonging table
 */
public class Fk1Fk2Props extends Tuple3<String, String, Properties> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Foreign key one
   */
  private String fk1;

  /**
   * Foreign key two
   */
  private String fk2;

  /**
   * Properties of n:m table
   */
  private Properties props;

  /**
   * Empty Constructor
   */
  public Fk1Fk2Props() { }

  /**
   * Constructor
   *
   * @param fk1 Name of foreign key one
   * @param fk2 Name of foreign key two
   * @param props Relation belonging properties
   */
  public Fk1Fk2Props(String fk1, String fk2, Properties props) {
    this.fk1 = fk1;
    this.f0 = fk1;
    this.fk2 = fk2;
    this.f1 = fk2;
    this.props = props;
    this.f2 = props;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((fk1 == null) ? 0 : fk1.hashCode());
    result = prime * result + ((fk2 == null) ? 0 : fk2.hashCode());
    result = prime * result + ((props == null) ? 0 : props.hashCode());
    return result;
  }

  /**
   * Checks if two Fk1Fk2Props tuples are equal
   *
   * @return <code>true</code> if Object equals Fk1Fk2Props; <code>false</code>
   * otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Fk1Fk2Props ffp = (Fk1Fk2Props) o;
    boolean equal = true;
    if (this.f0.equals(ffp.f0) && this.f1.equals(ffp.f1)) {
      for (Property p : props) {
        if (!ffp.f2.get(p.getKey()).equals(p.getValue())) {
          equal = false;
        }
      }
    }
    return equal;
  }

  public String getFk1() {
    return fk1;
  }

  public void setFk1(String fk1) {
    this.fk1 = fk1;
  }

  public String getFk2() {
    return fk2;
  }

  public void setFk2(String fk2) {
    this.fk2 = fk2;
  }

  public Properties getProps() {
    return props;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public static long getSerialversionuid() {
    return serialVersionUID;
  }
}
