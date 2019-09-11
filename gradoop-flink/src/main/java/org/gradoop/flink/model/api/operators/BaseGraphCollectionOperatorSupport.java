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
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;

/**
 * Defines methods for calling operators with this graph collection as a parameter.
 *
 * @param <LG> graph type
 * @param <GC> graph collection type
 */
public interface BaseGraphCollectionOperatorSupport<LG extends BaseGraph, GC extends BaseGraphCollection> {

  /**
   * Creates a value using the given unary collection to value operator.
   *
   * @param operator unary graph collection to value operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(UnaryBaseGraphCollectionToValueOperator<GC, T> operator);

  /**
   * Calls the given binary collection to value operator using this graph collection and the
   * input graph collection.
   *
   * @param operator        binary collection to value operator
   * @param otherCollection second input collection for operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(BinaryBaseGraphCollectionToValueOperator<GC, T> operator, GC otherCollection);

  /**
   * Creates a graph collection using the given unary graph collection operator.
   *
   * @param operator unary graph collection to graph collection operator
   * @return result of given operator
   */
  default GC callForCollection(UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> operator) {
    return callForValue(operator);
  }

  /**
   * Calls the given binary collection to collection operator using this graph collection and the
   * input graph collection.
   *
   * @param operator        binary collection to collection operator
   * @param otherCollection second input collection for operator
   * @return result of given operator
   */
  default GC callForCollection(BinaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> operator,
                               GC otherCollection) {
    return callForValue(operator, otherCollection);
  }

  /**
   * Applies a given unary graph to graph operator (e.g., aggregate) on each
   * base graph in the graph collection.
   *
   * @param operator applicable unary graph to graph operator
   * @return collection with resulting logical graphs
   */
  default GC apply(ApplicableUnaryBaseGraphToBaseGraphOperator<GC> operator) {
    return callForValue(operator);
  }

  /**
   * Transforms a graph collection into a graph by applying a
   * {@link ReducibleBinaryBaseGraphToBaseGraphOperator} pairwise on the elements of the collection.
   *
   * @param operator reducible binary graph to graph operator
   * @return base graph returned by the operator
   *
   * @see Exclusion
   * @see Overlap
   * @see Combination
   */
  default LG reduce(ReducibleBinaryBaseGraphToBaseGraphOperator<GC, LG> operator) {
    return callForValue(operator);
  }

  /**
   * Calls the given unary collection to graph operator for the collection.
   *
   * @param operator unary collection to graph operator
   * @return result of given operator
   */
  default LG callForGraph(UnaryBaseGraphCollectionToBaseGraphOperator<GC, LG> operator) {
    return callForValue(operator);
  }
}
