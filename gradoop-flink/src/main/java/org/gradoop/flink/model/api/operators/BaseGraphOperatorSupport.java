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
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;

/**
 * Defines callFor methods for graphs.
 *
 * @param <LG> graph type
 * @param <GC> graph collection type
 */
public interface BaseGraphOperatorSupport<LG extends BaseGraph, GC extends BaseGraphCollection> {

  /**
   * Creates a value using the given unary graph to value operator.
   *
   * @param operator unary graph to value operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(UnaryBaseGraphToValueOperator<LG, T> operator);

  /**
   * Calls the given binary graph to value operator using that graph and the input graph.
   *
   * @param operator   binary graph to value operator
   * @param otherGraph second input graph for operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(BinaryBaseGraphToValueOperator<LG, T> operator, LG otherGraph);

  /**
   * Creates a base graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  default LG callForGraph(UnaryBaseGraphToBaseGraphOperator<LG> operator) {
    return callForValue(operator);
  }

  /**
   * Creates a base graph from that graph and the input graph using the given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  default LG callForGraph(BinaryBaseGraphToBaseGraphOperator<LG> operator, LG otherGraph) {
    return callForValue(operator, otherGraph);
  }
  /**
   * Creates a graph collection from that graph using the given unary graph operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  default GC callForCollection(UnaryBaseGraphToBaseGraphCollectionOperator<LG, GC> operator) {
    return callForValue(operator);
  }

}
