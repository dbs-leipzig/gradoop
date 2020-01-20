/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToBaseGraphCollectionOperator;

/**
 * Abstract operator implementation which can be used with binary collection
 * to collection operators.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the logical graph instance
 * @param <GC> type of the graph collection
 */
public abstract class BinaryCollectionToCollectionOperatorBase<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements BinaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> {

  /**
   * First input collection.
   */
  protected GC firstCollection;
  /**
   * Second input collection.
   */
  protected GC secondCollection;

  @Override
  public GC execute(GC firstCollection, GC secondCollection) {

    // do some init stuff for the actual operator
    this.firstCollection = firstCollection;
    this.secondCollection = secondCollection;

    final DataSet<G> newGraphHeads = computeNewGraphHeads();
    final DataSet<V> newVertices = computeNewVertices(newGraphHeads);
    final DataSet<E> newEdges = computeNewEdges(newVertices);

    return firstCollection.getFactory()
      .fromDataSets(newGraphHeads, newVertices, newEdges);
  }

  /**
   * Overridden by inheriting classes.
   *
   * @param newGraphHeads new graph heads
   * @return vertex set of the resulting graph collection
   */
  protected abstract DataSet<V> computeNewVertices(DataSet<G> newGraphHeads);

  /**
   * Overridden by inheriting classes.
   *
   * @return subgraph dataset of the resulting collection
   */
  protected abstract DataSet<G> computeNewGraphHeads();

  /**
   * Overridden by inheriting classes.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   */
  protected abstract DataSet<E> computeNewEdges(DataSet<V> newVertices);

}
