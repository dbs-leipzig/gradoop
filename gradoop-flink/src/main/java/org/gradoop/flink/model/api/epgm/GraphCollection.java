/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.util.Order;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.difference.DifferenceBroadcast;
import org.gradoop.flink.model.impl.operators.equality.CollectionEquality;
import org.gradoop.flink.model.impl.operators.equality.CollectionEqualityByGraphIds;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.intersection.IntersectionBroadcast;
import org.gradoop.flink.model.impl.operators.limit.Limit;
import org.gradoop.flink.model.impl.operators.matching.transactional.TransactionalPatternMatching;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.selection.Selection;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.union.Union;

/**
 * A graph collection graph is one of the base concepts of the Extended Property Graph Model. From
 * a model perspective, the collection represents a set of logical graphs. From a data perspective
 * this is reflected by providing three concepts:
 *
 * - a set of graph heads assigned to the graphs in that collection
 * - a set of vertices which is the union of all vertex sets of the represented graphs
 * - a set of edges which is the union of all edge sets of the represented graphs
 *
 * Furthermore, a graph collection provides operations that are performed on the underlying data.
 * These operations result in either another graph collection or in a {@link LogicalGraph}.
 *
 * A graph collection is wrapping a {@link GraphCollectionLayout} which defines, how the collection
 * is represented in Apache Flink. Note that the GraphCollection also implements that interface and
 * just forward the calls to the layout. This is just for convenience and API synchronicity.
 */
public interface GraphCollection extends GraphCollectionOperators, GraphCollectionLayout {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  default GraphCollection select(final FilterFunction<GraphHead> predicate) {
    return callForCollection(new Selection(predicate));
  }

  default GraphCollection sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  default GraphCollection limit(int n) {
    return callForCollection(new Limit(n));
  }

  default GraphCollection match(String pattern, PatternMatchingAlgorithm algorithm,
    boolean returnEmbeddings) {
    return new TransactionalPatternMatching(
      pattern,
      algorithm,
      returnEmbeddings).execute(this);
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  default GraphCollection union(GraphCollection otherCollection) {
    return callForCollection(new Union(), otherCollection);
  }

  default GraphCollection intersect(GraphCollection otherCollection) {
    return callForCollection(new Intersection(), otherCollection);
  }

  default GraphCollection intersectWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new IntersectionBroadcast(),
      otherCollection);
  }

  default GraphCollection difference(GraphCollection otherCollection) {
    return callForCollection(new Difference(), otherCollection);
  }

  default GraphCollection differenceWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new DifferenceBroadcast(),
      otherCollection);
  }

  default DataSet<Boolean> equalsByGraphIds(GraphCollection other) {
    return new CollectionEqualityByGraphIds().execute(this, other);
  }

  default DataSet<Boolean> equalsByGraphElementIds(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(), true).execute(this, other);
  }

  default DataSet<Boolean> equalsByGraphElementData(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  default DataSet<Boolean> equalsByGraphData(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  default GraphCollection callForCollection(UnaryCollectionToCollectionOperator op) {
    return op.execute(this);
  }

  default GraphCollection callForCollection(BinaryCollectionToCollectionOperator op,
    GraphCollection otherCollection) {
    return op.execute(this, otherCollection);
  }

  default LogicalGraph callForGraph(UnaryCollectionToGraphOperator op) {
    return op.execute(this);
  }

  default GraphCollection apply(ApplicableUnaryGraphToGraphOperator op) {
    return callForCollection(op);
  }

  default LogicalGraph reduce(ReducibleBinaryGraphToGraphOperator op) {
    return callForGraph(op);
  }

  /**
   * Prints this graph collection to the console.
   *
   * @throws Exception forwarded DataSet print() Exception.
   */
  default void print() throws Exception {
    GDLConsoleOutput.print(this);
  }

}
