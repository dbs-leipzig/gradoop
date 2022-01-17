/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly;

import org.apache.flink.graph.Graph;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;

/**
 * Base class for Algorithms executed in Flink Gelly.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 * @param <K>  Key type of output gelly graph.
 * @param <VV> Value type of output gelly vertex.
 * @param <EV> Value type of output gelly edge.
 * @param <O>  Output type.
 */
public abstract class BaseGellyAlgorithm<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>,
  K, VV, EV, O> implements UnaryBaseGraphToValueOperator<LG, O> {

  @Override
  public O execute(LG graph) {
    try {
      return executeInGelly(transformToGelly(graph));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Default transformation from class K to a Gelly Graph.
   *
   * @param graph Gradoop Graph.
   * @return Gelly Graph.
   */
  public abstract Graph<K, VV, EV> transformToGelly(LG graph);

  /**
   * Perform some operation in Gelly and transform the Gelly graph back to a dedicated value.
   *
   * @param gellyGraph The Gelly graph.
   * @return output format.
   * @throws Exception on failure
   */
  public abstract O executeInGelly(Graph<K, VV, EV> gellyGraph) throws Exception;
}
