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

package org.gradoop.flink.model.impl.operators.distinction;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.DistinctionFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.distinction.functions.GraphHeadGroup;
import org.gradoop.flink.model.impl.operators.selection.SelectionBase;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * Returns a distinct collection of logical graphs.
 * Graphs are compared by isomorphism testing.
 */
public class GroupByIsomorphism extends SelectionBase {

  /**
   * Distinction function.
   */
  private final DistinctionFunction function;

  /**
   * Constructor.
   *
   * @param function reduce function to merge multiple heads of isomorphic graphs
   */
  public GroupByIsomorphism(DistinctionFunction function) {
    this.function = function;
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {
    DataSet<GraphHead> graphHeads = getCanonicalLabels(collection)
      .join(collection.getGraphHeads())
      .where(0).equalTo(new Id<>())
      .with(new GraphHeadGroup())
      .groupBy(0)
      .reduceGroup(function);

    return selectVerticesAndEdges(collection, graphHeads);
  }

  /**
   * Createas a canonical label for each graph in a collection.
   *
   * @param collection input collection
   * @return (graph id, label) pairs
   */
  protected DataSet<GraphHeadString> getCanonicalLabels(GraphCollection collection) {
    // Init builder for canonical labels
    CanonicalAdjacencyMatrixBuilder camBuilder = new CanonicalAdjacencyMatrixBuilder(
      new GraphHeadToEmptyString(),  new VertexToDataString(), new EdgeToDataString(), true);

    // create canonical labels for all graph heads and choose representative for all distinct ones
    return camBuilder
      .getGraphHeadStrings(collection);
  }

  @Override
  public String getName() {
    return GroupByIsomorphism.class.getName();
  }
}
