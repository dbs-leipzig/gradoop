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

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.operators.verify.VerifyGraphsContainment;

/**
 * Defines the operators that are available on a {@link BaseGraphCollection}.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> the type of the logical graph
 * @param <GC> the type of the graph collection
 */
public interface BaseGraphCollectionOperators<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Verifies this graph collection, removing dangling graph ids from its elements,
   * i.e. ids not contained in the collection.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this graph collection remains unchanged.
   *
   * @return this graph collection with all dangling graph ids removed.
   */
  default GC verifyGraphsContainment() {
    return callForCollection(new VerifyGraphsContainment<>());
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a graph collection using the given unary graph collection operator.
   *
   * @param operator unary graph collection to graph collection operator
   * @return result of given operator
   */
  GC callForCollection(UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> operator);
}
