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
package org.gradoop.flink.io.api;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;

import java.io.IOException;

/**
 * A data source providing an input for graphs and graph collections.
 *
 * @param <G> The graph head type of the graph.
 * @param <V> The vertex type of the graph.
 * @param <E> The edge type of the graph.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public interface BaseDataSource<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  /**
   * Read a graph from this source.
   *
   * @return The graph.
   * @throws IOException when reading the graph from this source fails.
   */
  LG getLogicalGraph() throws IOException;

  /**
   * Read a graph collection from this source.
   *
   * @return The graph collection.
   * @throws IOException when reading the graph collection from this source fails.
   */
  GC getGraphCollection() throws IOException;
}
