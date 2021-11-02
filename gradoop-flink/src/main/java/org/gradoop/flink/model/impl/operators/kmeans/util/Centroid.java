/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.kmeans.util;

import java.util.Objects;

/**
 * Centroid class, which is a specific point extended by an id.
 */
public class Centroid extends Point {

  /**
   * Unique ID of the centroid.
   */
  private int id;

  /**
   * Empty constructor of Centroid class
   */
  public Centroid() {
  }

  /**
   * Creates an instance of a centroid with an id, a latitude, and a longitude
   *
   * @param id  The unique id assigned to this centroid
   * @param lat The latitude of the centroid
   * @param lon The longitude of the centroid
   */
  public Centroid(int id, double lat, double lon) {
    super(lat, lon);
    this.id = id;
  }

  /**
   * Creates an instance of a centroid with an id and a point
   *
   * @param id The unique id assigned to the centroid
   * @param p  The point defining the coordinates of the centroid
   */
  public Centroid(int id, Point p) {
    super(p.getLat(), p.getLon());
    this.id = id;
  }

  /**
   * Gets id
   *
   * @return Returns the id of the centroid
   */
  public int getId() {
    return this.id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    Centroid centroid = (Centroid) o;
    return id == centroid.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), id);
  }

}
