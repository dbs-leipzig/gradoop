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

package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.fusion.reduce.ReduceVertexFusionOverGraphCollectionDataset;

/**
 * Reduces two {@link GraphCollection}s into a single {@link LogicalGraph}
 *
 * @see ReduceVertexFusionOverGraphCollectionDataset
 */
public interface GraphCollectionGraphCollectionToGraph extends Operator {

  /**
   * Combining {@link GraphCollection}s and a collection of graphs, we return a single logical graph
   *
   * @param left      Graph collection
   * @param right     Graph collection
   * @return          Single fused graph
   */
  LogicalGraph execute(GraphCollection left, GraphCollection right);

}
