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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.Serializable;

/**
 * Represents a centroid for repulsion-force computation for the CentroidFRLayouter algorithm.
 * Tuple-ID: Value <br>
 * 0: Id of the centroid <br>
 * 1: Position of the centroid <br>
 * 2: Number of associated vertices <br>
 */
public class Centroid extends Tuple3<GradoopId, Vector, Integer> implements SimpleGraphElement, Serializable {
  /**
   * Create a new Centroid. Id is chosen automatically.
   *
   * @param position Position of the centroid
   * @param count    Number of vertices associated to the centroid
   */
  public Centroid(Vector position, int count) {
    super(GradoopId.get(), position, count);
  }

  /**
   * Default constructor to conform with POJO-Rules
   */
  public Centroid() {
    super();
  }

  /**
   * Gets position
   *
   * @return value of position
   */
  public Vector getPosition() {
    return f1;
  }

  /**
   * Sets position
   *
   * @param position the new value
   */
  public void setPosition(Vector position) {
    this.f1 = position;
  }

  /**
   * Gets count
   *
   * @return value of count
   */
  public int getCount() {
    return f2;
  }

  /**
   * Sets count
   *
   * @param count the new value
   */
  public void setCount(int count) {
    this.f2 = count;
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
}
