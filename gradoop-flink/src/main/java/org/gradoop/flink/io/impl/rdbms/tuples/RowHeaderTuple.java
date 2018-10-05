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

package org.gradoop.flink.io.impl.rdbms.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Represents a tuple of a rowheader
 */
public class RowHeaderTuple extends Tuple3<String, String, Integer> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Name of attribute
   */
  private String name;

  /**
   * Data type of attribute
   */
  private String attType;

  /**
   * Position of attribute in row
   */
  private int pos;

  /**
   * Empty Constructor
   */
  public RowHeaderTuple() {
  }

  /**
   * Constructor
   *
   * @param name Attribute name
   * @param attType Attribute type
   * @param pos Attributes' position in table
   */
  public RowHeaderTuple(String name, String attType, int pos) {
    this.name = name;
    this.f0 = name;
    this.attType = attType;
    this.f1 = attType;
    this.pos = pos;
    this.f2 = pos;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((attType == null) ? 0 : attType.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + pos;
    return result;
  }

  /**
   * Checks if two RowHeaderTuple tuples are equal
   *
   * @param t object to check equality
   * @return <code>true</code> if Object equals RowHeaderTuple; <code>false</code>
   *         otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowHeaderTuple rht = (RowHeaderTuple) o;
    return this.f0.equals(rht.f0) && this.f1.equals(rht.f1) && this.f2 == rht.f2;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAttType() {
    return attType;
  }

  public void setAttType(String attType) {
    this.attType = attType;
  }

  public int getPos() {
    return pos;
  }

  public void setPos(int pos) {
    this.pos = pos;
  }
}
