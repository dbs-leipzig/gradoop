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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.ValueWeaklyConnectedComponents;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;

/**
 * Computes the weakly connected components of a graph structure. Uses the gradoop wrapper
 * {@link ValueWeaklyConnectedComponents} of Flinks ConnectedComponents.
 * <p>
 * Returns a mapping of {@code VertexId -> ComponentId}
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class ConnectedComponentsDistributionAsValues<
  G extends GraphHead,
  V extends org.gradoop.common.model.api.entities.Vertex,
  E extends org.gradoop.common.model.api.entities.Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToValueOperator<LG, DataSet<Tuple2<Long, Long>>> {

  /**
   * Max iterations.
   */
  private final int maxIteration;

  /**
   * Creates an instance of this operator to calculate the connected components distribution.
   *
   * @param maxiIteration max iteration count.
   */
  public ConnectedComponentsDistributionAsValues(int maxiIteration) {
    this.maxIteration = maxiIteration;
  }

  @Override
  public DataSet<Tuple2<Long, Long>> execute(LG graph) {
    return new ValueWeaklyConnectedComponents<G, V, E, LG, GC>(maxIteration).execute(graph);
  }
}
