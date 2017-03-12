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
 * Defines an item that has been reviewed
 */
public class Item extends Vertexable<String> {

  /**
   * Vertex id
   */
  private String id;

  /**
   * Initializing the default id
   */
  public Item() {
    id = "I";
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getLabel() {
    return "Item";
  }

  @Override
  public void updateByParse(String toParse) {
    throw new RuntimeException("Error: this method should never be invoked");
  }

  /**
   * Setter
   * @param id  Id
   */
  public void setId(String id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Item)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    Item that = (Item) o;

    return id != null ? id.equals(that.id) : that.id == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (id != null ? id.hashCode() : 0);
    return result;
  }
}
