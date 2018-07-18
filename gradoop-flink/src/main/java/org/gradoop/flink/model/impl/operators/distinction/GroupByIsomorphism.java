/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.distinction;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
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
  private final GraphHeadReduceFunction function;

  /**
   * Constructor.
   *
   * @param function reduce function to merge multiple heads of isomorphic graphs
   */
  public GroupByIsomorphism(GraphHeadReduceFunction function) {
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
