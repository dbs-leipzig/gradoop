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
package org.gradoop.flink.io.impl.gdl;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * Allows to print graphs and graph collections to the standard output.
 */
public class GDLConsoleOutput {

  /**
   * Prints the logical graph to the standard output.
   *
   * @param graph The logical graph that is supposed to be printed.
   * @param <G>   the graph head type
   * @param <V>   the vertex type
   * @param <E>   the edge type
   * @param <LG>  the type of the logical graph instance
   * @param <GC>  the type of the according graph collection
   * @throws Exception Forwarded from flink execute.
   */
  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> void print(BaseGraph<G, V, E, LG, GC> graph)
    throws Exception {

    print(graph.getCollectionFactory().fromGraph(graph));
  }

  /**
   * Prints the graph collection to the standard output.
   *
   * @param collection The graph collection that is supposed to be printed.
   * @param <G>        the graph head type
   * @param <V>        the vertex type
   * @param <E>        the edge type
   * @param <LG>       the type of the logical graph
   * @param <GC>       the type of the graph collection
   * @throws Exception Forwarded from flink execute.
   */
  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> void print(
      BaseGraphCollection<G, V, E, LG, GC> collection) throws Exception {

    List<G> graphHeads = new ArrayList<>();
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(graphHeads));

    List<V> vertices = new ArrayList<>();
    collection.getVertices().output(new LocalCollectionOutputFormat<>(vertices));

    List<E> edges = new ArrayList<>();
    collection.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    collection.getConfig().getExecutionEnvironment().execute();

    GDLEncoder encoder = new GDLEncoder<>(graphHeads, vertices, edges);
    String graphString = encoder.getGDLString();

    System.out.println(graphString);
  }
}
