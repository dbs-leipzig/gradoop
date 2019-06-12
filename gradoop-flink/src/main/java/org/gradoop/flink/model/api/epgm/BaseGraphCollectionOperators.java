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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToValueOperator;
import org.gradoop.flink.model.api.operators.ReducibleBinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.difference.DifferenceBroadcast;
import org.gradoop.flink.model.impl.operators.equality.CollectionEquality;
import org.gradoop.flink.model.impl.operators.equality.CollectionEqualityByGraphIds;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.intersection.IntersectionBroadcast;
import org.gradoop.flink.model.impl.operators.limit.Limit;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
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
    return callForCollection(new Union<>(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  default GC intersect(GC otherCollection) {
    return callForCollection(new Intersection<>(), otherCollection);
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
    return callForCollection(new IntersectionBroadcast<>(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that are contained in that
   * collection but not in the other. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  default GC difference(GC otherCollection) {
    return callForCollection(new Difference<>(), otherCollection);
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
    return callForCollection(new DifferenceBroadcast<>(), otherCollection);
  }

  /**
   * Checks, if another collection contains the same graphs as this graph (by id).
   *
   * @param otherCollection other graph collection
   * @return 1-element dataset containing true, if equal by graph ids
   */
  default DataSet<Boolean> equalsByGraphIds(GC otherCollection) {
    return callForCollection(new CollectionEqualityByGraphIds<>(), otherCollection);
  }

  /**
   * Checks, if another collection contains the same graphs as this graph (by vertex and edge ids).
   *
   * @param otherCollection other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  default DataSet<Boolean> equalsByGraphElementIds(GC otherCollection) {
    return callForCollection(new CollectionEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToIdString<>(),
      new EdgeToIdString<>(), true), otherCollection);
  }

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the graph collection is equal to the given graph collection.
   *
   * Equality is defined on the element data contained inside the collection,
   * i.e. vertices and edges.
   *
   * @param otherCollection graph collection to compare with
   * @return 1-element dataset containing {@code true} if the two collections
   * are equal or {@code false} if not
   */
  default DataSet<Boolean> equalsByGraphElementData(GC otherCollection) {
    return callForCollection(new CollectionEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true), otherCollection);
  }

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the graph collection is equal to the given graph collection.
   *
   * Equality is defined on the data contained inside the collection, i.e.
   * graph heads, vertices and edges.
   *
   * @param otherCollection graph collection to compare with
   * @return 1-element dataset containing {@code true} if the two collections
   * are equal or {@code false} if not
   */
  default DataSet<Boolean> equalsByGraphData(GC otherCollection) {
    return callForCollection(new CollectionEquality<>(
      new GraphHeadToDataString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true), otherCollection);
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

  /**
   * Calls the given binary collection to value operator using that
   * graph collection and the input graph collection.
   *
   * @param operator        binary collection to value operator
   * @param otherCollection second input collection for operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForCollection(BinaryBaseGraphCollectionToValueOperator<GC, T> operator,
                          GC otherCollection);

  /**
   * Calls the given binary collection to collection operator using that
   * graph collection and the input graph collection.
   *
   * @param operator        binary collection to collection operator
   * @param otherCollection second input collection for operator
   * @return result of given operator
   */
  GC callForCollection(BinaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> operator,
                       GC otherCollection);

  /**
   * Calls the given unary collection to graph operator for the collection.
   *
   * @param operator unary collection to graph operator
   * @return result of given operator
   */
  LG callForGraph(UnaryBaseGraphCollectionToBaseGraphOperator<GC, LG> operator);

  /**
   * Transforms a graph collection into a base graph by applying a
   * {@link BinaryBaseGraphToBaseGraphOperator} pairwise on the elements of the collection.
   *
   * @param operator reducible binary graph to graph operator
   * @return base graph
   *
   * @see Exclusion
   * @see Overlap
   * @see Combination
   */
  default LG reduce(ReducibleBinaryBaseGraphToBaseGraphOperator<GC, LG> operator) {
    return callForGraph(operator);
  }
}
