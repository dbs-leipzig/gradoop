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
 * A data sink providing an output for graphs and graph collections.
 *
 * @param <G> The graph head type of the graph.
 * @param <V> The vertex type of the graph.
 * @param <E> The edge type of the graph.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public interface BaseDataSink<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  /**
   * Writes a graph to this data sink.
   *
   * @param graph The graph to be written.
   * @throws IOException when outputting the graph fails.
   */
  void write(LG graph) throws IOException;

  /**
   * Writes a graph to this data sink with an option to overwrite existing data.
   *
   * @param graph     The graph to be written.
   * @param overwrite Overwrite existing data, when this option is set to {@code true}.
   * @throws IOException when outputting the graph fails.
   */
  void write(LG graph, boolean overwrite) throws IOException;

  /**
   * Write a graph collection to this data sink.
   *
   * @param graphCollection The graph collection to be written.
   * @throws IOException when outputting the graph collection fails.
   */
  void write(GC graphCollection) throws IOException;

  /**
   * Write a graph collection to this data sink with an option to overwrite existing data.
   *
   * @param graphCollection The graph collection to be written.
   * @param overwrite       Overwrite existing data, when this option is set to {@code true}.
   * @throws IOException when outputting the graph collection fails.
   */
  void write(GC graphCollection, boolean overwrite) throws IOException;
}
