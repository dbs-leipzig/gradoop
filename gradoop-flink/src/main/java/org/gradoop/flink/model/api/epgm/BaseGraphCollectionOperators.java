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

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.difference.DifferenceBroadcast;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.intersection.IntersectionBroadcast;
import org.gradoop.flink.model.impl.operators.limit.Limit;
import org.gradoop.flink.model.impl.operators.union.Union;
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
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Returns the first {@code n} arbitrary logical graphs contained in that
   * collection.
   *
   * @param n number of graphs to return from collection
   * @return subset of the graph collection
   */
  default GC limit(int n) {
    return callForCollection(new Limit<>(n));
  }

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
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Returns a collection with all base graphs from two input collections.
   * Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build union with
   * @return union of both collections
   */
  default GC union(GC otherCollection) {
    return callForCollection(new Union(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  default GC intersect(GC otherCollection) {
    return callForCollection(new Intersection(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   * <p>
   * Implementation that works faster if {@code otherCollection} is small
   * (e.g. fits in the workers main memory).
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  default GC intersectWithSmallResult(GC otherCollection) {
    return callForCollection(new IntersectionBroadcast(),otherCollection);
  }

  /**
   * Returns a collection with all base graphs that are contained in that
   * collection but not in the other. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  default GC difference(GC otherCollection) {
    return callForCollection(new Difference(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that are contained in that
   * collection but not in the other. Graph equality is based on their identifiers.
   * <p>
   * Alternate implementation that works faster if the intermediate result
   * (list of graph identifiers) fits into the workers memory.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  default GC differenceWithSmallResult(GC otherCollection) {
    return callForCollection(new DifferenceBroadcast(), otherCollection);
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
