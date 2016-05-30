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

package org.gradoop.io.graphgen.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Represents a vertex used in a graph generation from GraphGen-files.
 */
public class GraphGenVertex extends Tuple2<Integer, String> {

  /**
   * default constructor
   */
  public GraphGenVertex() {
  }

  /**
   * valued constructor
   * @param id vertex id
   * @param label vertex label
   */
  public GraphGenVertex(Integer id, String label) {
    setId(id);
    setLabel(label);
  }

  public Integer getId() {
    return this.f0;
  }

  public void setId(Integer id) {
    this.f0 = id;
  }

  public String getLabel() {
    return this.f1;
  }

  public void setLabel(String label) {
    this.f1 = label;
  }
}
