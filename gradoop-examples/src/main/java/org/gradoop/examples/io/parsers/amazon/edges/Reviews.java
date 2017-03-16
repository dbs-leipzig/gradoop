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
package org.gradoop.examples.io.parsers.amazon.edges;

import org.gradoop.examples.io.parsers.inputfilerepresentations.Edgable;

/**
 * Connects a Reviewer to an Item
 */
public class Reviews extends Edgable<String> {

  /**
   * Vertex source
   */
  private String src;
  /**
   * Vertex destination
   */
  private String dst;

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
    return "UserReviewsItem";
  }

  @Override
  public void updateByParse(String toParse) {
    throw new RuntimeException("Error: this method should never be invoked");
  }

  /**
   * Setter
   * @param dst Destination vertex
   */
  public void setDst(String dst) {
    this.dst = dst;
  }

  /**
   * Setter
   * @param src Source vertex
   */
  public void setSrc(String src) {
    this.src = src;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Reviews)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    Reviews that = (Reviews) o;

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
