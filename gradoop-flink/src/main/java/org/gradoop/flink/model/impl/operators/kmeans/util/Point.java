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

import java.io.Serializable;
import java.util.Objects;

/**
 * Point class that can be created by defining its coordinates.
 * Extends the existing code snippet from GitHub repository
 * https://github.com/TU-Berlin-DIMA/flink-tutorial.git
 */
public class Point implements Serializable {

  /**
   * Latitude coordinate of the point
   */
  private double lat;

  /**
   * Longitude coordinate of the point
   */
  private double lon;

  /**
   * Empty constructor of Point class
   */
  public Point() {
  }

  /**
   * Creates a point with the coordinates assigned to it.
   *
   * @param lat First spatial property of the point
   * @param lon Second spatial property of thw point
   */
  public Point(double lat, double lon) {
    this.lat = lat;
    this.lon = lon;
  }

  /**
   * Defines how to sum up two points. Done by summing up the latitudes and longitudes of each point.
   *
   * @param other Point that is added
   * @return Returns a point with the summed up longitudes and latitudes
   */
  public Point add(Point other) {
    this.lat += other.getLat();
    this.lon += other.getLon();
    return this;
  }

  /**
   * Computes the euclidean distance between two points.
   *
   * @param other Point to which the distance is computed
   * @return Returns the euclidean distance between the two points
   */
  public double euclideanDistance(Point other) {
    return Math.sqrt(
      (lat - other.getLat()) * (lat - other.getLat()) + (lon - other.getLon()) * (lon - other.getLon()));
  }

  /**
   * Defines how a point is divided by a value. Done by dividing the latitude and longitude by the value.
   *
   * @param val Value the points is divided by
   * @return Returns the point with its divided latitude and longitude
   */
  public Point div(long val) {
    lat /= val;
    lon /= val;
    return this;
  }

  /**
   * Gets the latitude.
   *
   * @return Returns the value of the latitude
   */
  public Double getLat() {
    return this.lat;
  }

  /**
   * Gets Longitude
   *
   * @return Returns the value of the longitude
   */
  public double getLon() {
    return this.lon;
  }

  /**
   * Sets Latitude
   *
   * @param lat New value for lat
   */
  public void setLat(double lat) {
    this.lat = lat;
  }

  /**
   * Sets Longitude
   *
   * @param lon New value for lon
   */
  public void setLon(double lon) {
    this.lon = lon;
  }


  /**
   * Equals method implemented to compare two points.
   *
   * @param o Other point this point is compared to
   * @return Returns if the points are equal
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Point point = (Point) o;
    return Double.compare(point.getLat(), lat) == 0 && Double.compare(point.getLon(), lon) == 0;
  }

  /**
   * HashCode method implemented to compare two points.
   *
   * @return Returns a unique hash for this instance of point.
   */
  @Override
  public int hashCode() {
    return Objects.hash(lat, lon);
  }
}
