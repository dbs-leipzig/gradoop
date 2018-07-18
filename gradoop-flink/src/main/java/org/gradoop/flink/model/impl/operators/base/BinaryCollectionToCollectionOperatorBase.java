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
package org.gradoop.flink.model.impl.operators.base;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.BinaryCollectionToCollectionOperator;

/**
 * Abstract operator implementation which can be used with binary collection
 * to collection operators.
 */
public abstract class BinaryCollectionToCollectionOperatorBase
  implements BinaryCollectionToCollectionOperator {

  /**
   * First input collection.
   */
  protected GraphCollection firstCollection;
  /**
   * Second input collection.
   */
  protected GraphCollection secondCollection;

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection execute(
    GraphCollection firstCollection,
    GraphCollection secondCollection) {

    // do some init stuff for the actual operator
    this.firstCollection = firstCollection;
    this.secondCollection = secondCollection;

    final DataSet<GraphHead> newGraphHeads = computeNewGraphHeads();
    final DataSet<Vertex> newVertices = computeNewVertices(newGraphHeads);
    final DataSet<Edge> newEdges = computeNewEdges(newVertices);

    return firstCollection.getConfig().getGraphCollectionFactory()
      .fromDataSets(newGraphHeads, newVertices, newEdges);
  }

  /**
   * Overridden by inheriting classes.
   *
   * @param newGraphHeads new graph heads
   * @return vertex set of the resulting graph collection
   */
  protected abstract DataSet<Vertex> computeNewVertices(
    DataSet<GraphHead> newGraphHeads);

  /**
   * Overridden by inheriting classes.
   *
   * @return subgraph dataset of the resulting collection
   */
  protected abstract DataSet<GraphHead> computeNewGraphHeads();

  /**
   * Overridden by inheriting classes.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   */
  protected abstract DataSet<Edge> computeNewEdges(DataSet<Vertex> newVertices);

}
