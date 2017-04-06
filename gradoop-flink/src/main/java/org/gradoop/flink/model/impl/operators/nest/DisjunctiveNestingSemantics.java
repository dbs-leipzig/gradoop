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

package org.gradoop.flink.model.impl.operators.nest;

import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.model.NestedModel;

/**
 * Implements the nest operator for the EPGM data model. Given a graph describing the groundtruth
 * information and a collection of graph representing the mined patterns, it returns a nested graph
 * where each vertex is either a vertex representing a graph in the graph collection that contains
 * at least one match with the ground truth or a non-matcher vertex. The edges from the former
 * graph are also inherited.
 */
public class DisjunctiveNestingSemantics implements GraphGraphCollectionToGraphOperator {

  /**
   * Defines the model where the elements are set
   */
  private final NestedModel model;

  public DisjunctiveNestingSemantics(NestedModel model) {
    this.model = model;
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public LogicalGraph execute(LogicalGraph nestedGraph, GraphCollection collection) {
    return null;
  }
}
