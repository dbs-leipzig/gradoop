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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.io.Serializable;

/**
 * Each triple represents an edge coming from one of the graph operands joining possibly two
 * new edges coming from the to-be-returned graph.
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public class Triple extends Tuple3<Vertex, Edge, Vertex> implements Serializable {
  /**
   * Default constructor
   * @param f0      Source vertex belonging to the to-be-returned graph
   * @param f1      Edge belonging to one of the operands, matching with the new source and
   *                destination vertice
   * @param f11     Destination vertex belonging to the to-be-returned graph
   */
  public Triple(Vertex f0, Edge f1, Vertex f11) {
    super(f0, f1, f11);
  }

  /**
   * Required element-free constructor, (otherwise Apache Flink curses…)
   */
  public Triple() {  }
}
