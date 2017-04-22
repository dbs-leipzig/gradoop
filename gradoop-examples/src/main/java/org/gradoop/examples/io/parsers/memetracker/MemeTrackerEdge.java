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

package org.gradoop.examples.io.parsers.memetracker;

import org.gradoop.examples.io.parsers.inputfilerepresentations.Edgable;

/**
 * Defining the edge parsed from the Adjacency List
 */
public class MemeTrackerEdge extends Edgable<String> {

  /**
   * Source
   */
  private String src;

  /**
   * Destination
   */
  private String dst;

  /**
   * Default constructor
   * @param src Edge source
   * @param dst Edge Edge
   */
  public MemeTrackerEdge(String src, String dst) {
    this.src = src;
    this.dst = dst;
  }

  @Override
  public String getSourceVertexId() {
    return src;
  }

  @Override
  public String getTargetVertexId() {
    return dst;
  }

  @Override
  public String getLabel() {
    return "RefersTo";
  }

  @Override
  public void updateByParse(String toParse) {
    // noop
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MemeTrackerEdge)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    MemeTrackerEdge that = (MemeTrackerEdge) o;

    if (src != null ? !src.equals(that.src) : that.src != null) {
      return false;
    }
    return dst != null ? dst.equals(that.dst) : that.dst == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (src != null ? src.hashCode() : 0);
    result = 31 * result + (dst != null ? dst.hashCode() : 0);
    return result;
  }
}
