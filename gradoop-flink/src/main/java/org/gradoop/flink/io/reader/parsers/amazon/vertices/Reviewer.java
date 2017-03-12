/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.reader.parsers.amazon.vertices;

import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.Vertexable;

/**
 * Defines a reivewer reviewing an element
 */
public class Reviewer extends Vertexable<String> implements Comparable<Reviewer> {

  /**
   * Vertex id
   */
  private String rewId;

  /**
   * Vertex name
   */
  private String rewName;

  /**
   * Default empty constructor
   */
  public Reviewer() {
    this.rewId = "R";
    this.rewName = "";
  }

  /**
   * Setting…
   * @param rewId the vertex id
   */
  public void setRewId(String rewId) {
    this.rewId = rewId;
  }

  /**
   * Setting…
   * @param rewName the user's name
   */
  public void setRewName(String rewName) {
    this.rewName = rewName;
  }

  @Override
  public int compareTo(Reviewer o) {
    return rewId.compareTo(o.rewId);
  }

  @Override
  public String getId() {
    return rewId;
  }

  @Override
  public String getLabel() {
    return "User";
  }

  @Override
  public void updateByParse(String toParse) {
    throw new RuntimeException("Error: this method should never be invoked");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Reviewer)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    Reviewer that = (Reviewer) o;

    if (rewId != null ? !rewId.equals(that.rewId) : that.rewId != null) {
      return false;
    }
    return rewName != null ? rewName.equals(that.rewName) : that.rewName == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (rewId != null ? rewId.hashCode() : 0);
    result = 31 * result + (rewName != null ? rewName.hashCode() : 0);
    return result;
  }
}
