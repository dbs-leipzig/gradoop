/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting.util;


import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Represents a force that is applied to a vertex
 */
public class Force extends Tuple2<GradoopId, Vector> {
  /**
   * Position of the ID-property in the tuple
   */
  public static final int ID_POSITION = 0;
  /**
   * Create a new Force-Object
   *
   * @param id    The id of the vertex that the force should be applied t
   * @param value The force to apply
   */
  public Force(GradoopId id, Vector value) {
    super(id, value);
  }

  /**
   * POJO-Constructor
   */
  public Force() {
    super();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public GradoopId getId() {
    return f0;
  }

  /**
   * Sets id
   *
   * @param id the new value
   */
  public void setId(GradoopId id) {
    this.f0 = id;
  }

  /**
   * Gets value
   *
   * @return value of value
   */
  public Vector getValue() {
    return f1;
  }

  /**
   * Sets value
   *
   * @param value the new value
   */
  public void setValue(Vector value) {
    this.f1 = value;
  }

  /**
   * Set id and value at once. Useful for functions that reuse objects
   *
   * @param id    Id to set
   * @param value Value to set
   */
  public void set(GradoopId id, Vector value) {
    this.f0 = id;
    this.f1 = value;
  }

  /**
   * Create a (deep) copy of this object
   *
   * @return a (deep) copy of this object
   */
  public Force copy() {
    return new Force(getId(), getValue().copy());
  }
}
