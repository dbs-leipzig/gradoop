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
 * Indicates that a graph is writeable.
 *
 * @param <G> The graph head type of the graph.
 * @param <V> The vertex type of the graph.
 * @param <E> The edge type of the graph.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public interface Writeable<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  /**
   * Writes graph/graph collection to given data sink.
   *
   * @param dataSink data sink
   * @throws IOException if the data sink can't be written
   */
  void writeTo(BaseDataSink<G, V, E, LG, GC> dataSink) throws IOException;

  /**
   * Writes logical graph/graph collection to given data sink with overwrite option
   *
   * @param dataSink data sink
   * @param overWrite determines whether existing files are overwritten
   * @throws IOException if the data sink can't be written
   */
  void writeTo(BaseDataSink<G, V, E, LG, GC> dataSink, boolean overWrite) throws IOException;
}
