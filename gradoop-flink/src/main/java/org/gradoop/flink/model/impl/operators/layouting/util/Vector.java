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

import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

import java.io.Serializable;


/**
 * Simple helper-class for some vector-math.
 * All math-operations will return a new Vector (instead of modifying the existing vector). This
 * prevents strange side-effects when performing complex computations.
 */
public class Vector implements Serializable {

  /**
   * X-coordinate of vector
   */
  private double x;
  /**
   * Y-coordinate of vector
   */
  private double y;

  /**
   * Construct a vector from x and y coordinates
   *
   * @param x X-Coordinate of the new vector
   * @param y Y-Coordinate of the new vector
   */
  public Vector(double x, double y) {
    this.x = x;
    this.y = y;
    check();
  }

  /**
   * Construct new zero-Vector
   */
  public Vector() {
    x = 0d;
    y = 0d;
  }

  /**
   * Ensure that this vector does not contain any NaN-values.
   * Throws IllegalStateException if x or y is NaN.
   */
  private void check() {
    if (Double.isNaN(x) || Double.isNaN(y)) {
      throw new IllegalStateException(
        "A vector should never be NaN. There is probably a bug in " + "your code");
    }
  }

  /**
   * Create a vector from the coordinate-properties of a Vertex
   *
   * @param v The vertex to extract position coordinates from properties (X,Y)
   * @return A matching vector
   */
  public static Vector fromVertexPosition(EPGMVertex v) {
    double x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
    double y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
    return new Vector(x, y);
  }

  /**
   * Set the coordinate-properties of a vertex to the values of this vector
   *
   * @param v The vertex that will receive the values of this vector as coordinates
   */
  public void setVertexPosition(EPGMVertex v) {
    v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY, (int) x);
    v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY, (int) y);
  }

  /**
   * Substract another vector from this vector and return the result
   *
   * @param other Vector to substract
   * @return this-other
   */
  public Vector sub(Vector other) {
    return new Vector(x - other.x, y - other.y);
  }

  /**
   * Add another vector to this vector and return the result
   *
   * @param other The vector to add
   * @return this+other
   */
  public Vector add(Vector other) {
    return new Vector(x + other.x, y + other.y);
  }

  /**
   * Multiply this vector by a factor and return the result
   *
   * @param factor The factor to multiply this vector with
   * @return this*factor
   */
  public Vector mul(double factor) {
    return new Vector(x * factor, y * factor);
  }

  /**
   * Divide this vector by a factor and return the result
   *
   * @param factor The factor to divide this vector by
   * @return this/factor
   */
  public Vector div(double factor) {
    return new Vector(x / factor, y / factor);
  }

  /**
   * Calculate the euclidean distance between this vector and another vector
   *
   * @param other The other vector
   * @return Math.sqrt(Math.pow ( x - other.x, 2) + Math.pow(y - other.y, 2))
   */
  public double distance(Vector other) {
    return Math.sqrt(Math.pow(x - other.x, 2) + Math.pow(y - other.y, 2));
  }

  /**
   * Calculate the scalar-product of this vector
   *
   * @param other The other vector
   * @return Skalar-product of this and other
   */
  public double scalar(Vector other) {
    return x * other.x + y * other.y;
  }

  /**
   * Clamp this vector to a given length.
   * The returned vector will have the same orientation as this one, but will have at most a
   * length of maxLen.
   * If maxLen is smaller the the lenght of this Vector this vector (a copy of it) will be returned.
   *
   * @param maxLen maximum lenght of vector
   * @return This vector but constrained to the given length
   */
  public Vector clamped(double maxLen) {
    double len = magnitude();
    if (len == 0) {
      return new Vector(0, 0);
    }
    double newx = (x / len) * Math.min(len, maxLen);
    double newy = (y / len) * Math.min(len, maxLen);
    return new Vector(newx, newy);
  }

  /**
   * Normalize this vector.
   *
   * @return a vector with the same orientation as this one and a length of 1. If this vector is
   * (0,0) then (0,0) will be returned instead.
   */
  public Vector normalized() {
    double len = magnitude();
    if (len == 0) {
      return new Vector(0, 0);
    }
    double newx = x / len;
    double newy = y / len;
    return new Vector(newx, newy);
  }

  /**
   * Get the lenght of this vector
   *
   * @return euclidean length of this vector
   */
  public double magnitude() {
    return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
  }

  /**
   * Confine this point to the given bounding-box.
   *
   * @param minX Bounding-box
   * @param maxX Bounding-box
   * @param minY Bounding-box
   * @param maxY Bounding-box
   * @return A vector that does not violate the given bounding box.
   */
  public Vector confined(double minX, double maxX, double minY, double maxY) {
    double newx = Math.min(Math.max(x, minX), maxX);
    double newy = Math.min(Math.max(y, minY), maxY);
    return new Vector(newx, newy);
  }

  /**
   * Calculates the (unsigned) angle between this vector and the given vecotr
   *
   * @param other The other vector
   * @return The angle
   */
  public double angle(Vector other) {
    return Math.acos(scalar(other) / (magnitude() * other.magnitude())) / Math.PI * 180;
  }

  /**
   * Rotate this vector anti-clockwise by the given angle (in degrees)
   *
   * @param angle The angle o rotate this vector by
   * @return The rotated vector
   */
  public Vector rotate(double angle) {
    angle = angle / 180 * Math.PI;
    double newx = x * Math.cos(angle) - y * Math.sin(angle);
    double newy = x * Math.sin(angle) + y * Math.cos(angle);
    return new Vector(newx, newy);
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof Vector) {
      Vector otherv = (Vector) other;
      return this.distance(otherv) < 0.000000001;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return ((int) x << 16) + (int) y;
  }

  @Override
  public String toString() {
    return "Vector{" + "x=" + x + ", y=" + y + '}';
  }

  /** X-coordinate of vector
   *
   * @return X coordinate of the vector
   **/
  public double getX() {
    return x;
  }

  /** Set X-coordinate of vector
   * @param x Set X coordinate of the vector
   **/
  public void setX(double x) {
    this.x = x;
    check();
  }

  /** Y-Coordinate of vector
   * @return Y coordinate of the vector
   */
  public double getY() {
    return y;
  }

  /** Set Y-Coordinate of vector
   * @param y Set Y coordinate of the vector
   **/
  public void setY(double y) {
    this.y = y;
    check();
  }

  /**
   * Set x and y at once
   *
   * @param x X to set
   * @param y y to set
   * @return This object for method-chaining
   */
  public Vector set(double x, double y) {
    this.x = x;
    this.y = y;
    check();
    return this;
  }

  /**
   * Copy the values of the other vector into this one
   *
   * @param other The other vector
   * @return this
   */
  public Vector set(Vector other) {
    x = other.x;
    y = other.y;
    check();
    return this;
  }

  /**
   * Reset this vector to 0
   *
   * @return This object for method-chaining
   */
  public Vector reset() {
    x = 0d;
    y = 0d;
    return this;
  }

  /**
   * Copy this object
   *
   * @return A copy of this object
   */
  public Vector copy() {
    return new Vector(x, y);
  }

  //-----------------------------------------------------------------------------------

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Substract another vector from this vector and return the result
   *
   * @param other Vector to substract
   * @return this-other
   */
  public Vector mSub(Vector other) {
    x -= other.x;
    y -= other.y;
    check();
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Add another vector to this vector and return the result
   *
   * @param other The vector to add
   * @return this+other
   */
  public Vector mAdd(Vector other) {
    x += other.x;
    y += other.y;
    check();
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Multiply this vector by a factor and return the result
   *
   * @param factor The factor to multiply this vector with
   * @return this*factor
   */
  public Vector mMul(double factor) {
    x *= factor;
    y *= factor;
    check();
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Divide this vector by a factor and return the result
   *
   * @param factor The factor to divide this vector by
   * @return this/factor
   */
  public Vector mDiv(double factor) {
    x /= factor;
    y /= factor;
    check();
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Clamp this vector to a given length.
   * The returned vector will have the same orientation as this one, but will have at most a
   * length of maxLen.
   * If maxLen is smaller the the lenght of this Vector this vector (a copy of it) will be returned.
   *
   * @param maxLen maximum lenght of vector
   * @return This vector but constrained to the given length
   */
  public Vector mClamped(double maxLen) {
    double len = magnitude();
    if (len == 0) {
      return new Vector(0, 0);
    }
    x = (x / len) * Math.min(len, maxLen);
    y = (y / len) * Math.min(len, maxLen);
    check();
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Normalize this vector.
   *
   * @return a vector with the same orientation as this one and a length of 1. If this vector is
   * (0,0) then (0,0) will be returned instead.
   */
  public Vector mNormalized() {
    double len = magnitude();
    if (len == 0) {
      x = 0.0;
      y = 0.0;
      return this;
    }
    x /= len;
    y /= len;
    check();
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Confine this point to the given bounding-box.
   *
   * @param minX Bounding-box
   * @param maxX Bounding-box
   * @param minY Bounding-box
   * @param maxY Bounding-box
   * @return A vector that does not violate the given bounding box.
   */
  public Vector mConfined(double minX, double maxX, double minY, double maxY) {
    x = Math.min(Math.max(x, minX), maxX);
    y = Math.min(Math.max(y, minY), maxY);
    check();
    return this;
  }

  /**
   * Alternative MUTATING variant.  Rotate this vector anti-clockwise by the given angle (in
   * degrees)
   *
   * @param angle The angle o rotate this vector by
   * @return The rotated vector
   */
  public Vector mRotate(double angle) {
    angle = angle / 180 * Math.PI;
    double newx = x * Math.cos(angle) - y * Math.sin(angle);
    double newy = x * Math.sin(angle) + y * Math.cos(angle);
    x = newx;
    y = newy;
    check();
    return this;
  }

}
