/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.impl.functions.epgm.BySameId;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Arrays;

/**
 * Default interface of a EPGM graph collection instance.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> the type of the logical graph
 * @param <GC> the type of the graph collection that will be created with a provided factory
 */
public interface BaseGraphCollection<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  extends GraphCollectionLayout<G, V, E>, BaseGraphCollectionOperators<G, V, E, LG, GC> {

  /**
   * Returns the Gradoop Flink configuration.
   *
   * @return the Gradoop Flink configuration
   */
  GradoopFlinkConfig getConfig();

  /**
   * Get the factory that is responsible for creating an instance of {@link GC}.
   *
   * @return a factory that can be used to create a {@link GC} instance
   */
  BaseGraphCollectionFactory<G, V, E, LG, GC> getFactory();

  //----------------------------------------------------------------------------
  // Base Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  @Override
  default LG getGraph(final GradoopId graphID) {
    // filter vertices and edges based on given graph id
    DataSet<G> graphHead = getGraphHeads()
      .filter(new BySameId<>(graphID));
    DataSet<V> vertices = getVertices()
      .filter(new InGraph<>(graphID));
    DataSet<E> edges = getEdges()
      .filter(new InGraph<>(graphID));

    return getGraphFactory().fromDataSets(graphHead, vertices, edges);
  }

  @Override
  default GC getGraphs(final GradoopId... identifiers) {
    GradoopIdSet graphIds = new GradoopIdSet();
    graphIds.addAll(Arrays.asList(identifiers));

    return getGraphs(graphIds);
  }

  @Override
  default GC getGraphs(final GradoopIdSet identifiers) {
    DataSet<G> newGraphHeads = this.getGraphHeads()
      .filter((FilterFunction<G>) graphHead -> identifiers.contains(graphHead.getId()));

    // build new vertex set
    DataSet<V> vertices = getVertices()
      .filter(new InAnyGraph<>(identifiers));

    // build new edge set
    DataSet<E> edges = getEdges()
      .filter(new InAnyGraph<>(identifiers));

    return getFactory().fromDataSets(newGraphHeads, vertices, edges);
  }
}
