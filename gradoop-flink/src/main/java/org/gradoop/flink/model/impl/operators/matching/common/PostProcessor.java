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
package org.gradoop.flink.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.EdgeFromIds;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.MergedGraphIds;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromId;
import org.gradoop.flink.model.impl.functions.utils.Cast;
import org.gradoop.flink.model.impl.functions.utils.IsInstance;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.EdgeTriple;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;

/**
 * Provides methods for post-processing query results.
 */
public class PostProcessor {

  /**
   * Extracts a {@link GraphCollection} from a set of {@link Element}.
   *
   * @param elements   elements
   * @param factory    element factory
   * @param mayOverlap elements may be contained in multiple graphs
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return Graph collection
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> GC extractGraphCollection(
    DataSet<Element> elements, BaseGraphCollectionFactory<G, V, E, LG, GC> factory, boolean mayOverlap) {

    Class<G> graphHeadType = factory.getGraphHeadFactory().getType();
    Class<V> vertexType = factory.getVertexFactory().getType();
    Class<E> edgeType = factory.getEdgeFactory().getType();
    return factory.fromDataSets(
      extractGraphHeads(elements, graphHeadType),
      extractVertices(elements, vertexType, mayOverlap),
      extractEdges(elements, edgeType, mayOverlap)
    );
  }

  /**
   * Extracts a {@link GraphCollection} from a set of {@link Element} and
   * attaches the original data from the input {@link LogicalGraph}.
   *
   * @param elements      elements
   * @param inputGraph    original input graph
   * @param mayOverlap    true, if elements may be contained in multiple graphs
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return Graph collection
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> GC extractGraphCollectionWithData(
    DataSet<Element> elements, LG inputGraph, boolean mayOverlap) {

    BaseGraphCollectionFactory<G, V, E, LG, GC> factory = inputGraph.getCollectionFactory();

    // get result collection without data
    GC collection = extractGraphCollection(elements, factory, mayOverlap);

    // attach data by joining first and merging the graph head ids
    DataSet<V> newVertices = inputGraph.getVertices()
      .rightOuterJoin(collection.getVertices())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new MergedGraphIds<>())
      .withForwardedFieldsFirst("id;label;properties;");


    DataSet<E> newEdges = inputGraph.getEdges()
      .rightOuterJoin(collection.getEdges())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new MergedGraphIds<>())
      .withForwardedFieldsFirst("id;label;properties");

    return factory.fromDataSets(collection.getGraphHeads(), newVertices, newEdges);
  }
  /**
   * Extracts vertex ids from the given pattern matching result.
   *
   * @param result pattern matching result
   * @return dataset with (vertexId) tuples
   */
  private static DataSet<Tuple1<GradoopId>> extractVertexIds(
    DataSet<FatVertex> result) {
    return result.project(0);
  }

  /**
   * Extracts edge-source-target id triples from the given pattern matching
   * result.
   *
   * @param result pattern matching result
   * @return dataset with (edgeId, sourceId, targetId) tuples
   */
  private static DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> extractEdgeIds(
    DataSet<FatVertex> result) {
    return result.flatMap(new EdgeTriple());
  }

  /**
   * Filters and casts graph heads from a given set of {@link Element}
   *
   * @param elements      elements
   * @param graphHeadType graph head type
   * @param <G> The graph head type
   * @return graph heads
   */
  private static <G extends GraphHead> DataSet<G> extractGraphHeads(DataSet<Element> elements,
    Class<G> graphHeadType) {
    return elements
      .filter(new IsInstance<>(graphHeadType))
      .map(new Cast<>(graphHeadType))
      .returns(TypeExtractor.createTypeInfo(graphHeadType));
  }

  /**
   * Initializes vertices from the given pattern matching result.
   *
   * @param result        pattern matching result
   * @param vertexFactory vertex factory
   * @param <V> The produced vertex type.
   * @return vertices
   */
  public static <V extends Vertex> DataSet<V> extractVertices(DataSet<FatVertex> result,
    VertexFactory<V> vertexFactory) {
    return extractVertexIds(result).map(new VertexFromId<>(vertexFactory));
  }

  /**
   * Filters and casts vertices from a given set of {@link Element}
   *
   * @param elements      elements
   * @param vertexType    vertex type
   * @param mayOverlap    vertices may be contained in multiple graphs
   * @param <V> The produced vertex type.
   * @return vertices
   */
  private static <V extends Vertex> DataSet<V> extractVertices(DataSet<Element> elements,
    Class<V> vertexType, boolean mayOverlap) {
    DataSet<V> result = elements
      .filter(new IsInstance<>(vertexType))
      .map(new Cast<>(vertexType))
      .returns(TypeExtractor.createTypeInfo(vertexType));
    // TODO: Replace two group-by statements with a combinable reduce function.
    return mayOverlap ? result
      .groupBy(new Id<>())
      .combineGroup(new MergedGraphIds<>())
      .groupBy(new Id<>())
      .reduceGroup(new MergedGraphIds<>()) : result;
  }

  /**
   * Initializes edges from the given pattern matching result.
   *
   * @param result      pattern matching result
   * @param edgeFactory edge factory
   * @param <E> The produced edge type.
   * @return edges
   */
  public static <E extends Edge> DataSet<E> extractEdges(DataSet<FatVertex> result,
    EdgeFactory<E> edgeFactory) {
    return extractEdgeIds(result).map(new EdgeFromIds<>(edgeFactory));
  }

  /**
   * Filters and casts edges from a given set of {@link Element}
   *
   * @param elements      elements
   * @param edgeType      edge type
   * @param mayOverlap    edges may be contained in multiple graphs
   * @param <E> The produced edge type.
   * @return edges
   */
  private static <E extends Edge> DataSet<E> extractEdges(DataSet<Element> elements,
    Class<E> edgeType, boolean mayOverlap) {
    DataSet<E> result = elements
      .filter(new IsInstance<>(edgeType))
      .map(new Cast<>(edgeType))
      .returns(TypeExtractor.createTypeInfo(edgeType));

    return mayOverlap ? result
      .groupBy(new Id<>())
      .combineGroup(new MergedGraphIds<>()).groupBy(new Id<>())
      .reduceGroup(new MergedGraphIds<>()) : result;
  }

  /**
   * Initializes vertices including their original data from the given
   * pattern matching result.
   *
   * @param result        pattern matching result
   * @param inputVertices original data graph vertices
   * @param <V> The vertex type.
   * @return vertices including data
   */
  public static <V extends Vertex> DataSet<V> extractVerticesWithData(
    DataSet<FatVertex> result, DataSet<V> inputVertices) {
    return extractVertexIds(result)
      .join(inputVertices)
      .where(0).equalTo(new Id<>())
      .with(new RightSide<>());
  }

  /**
   * Initializes edges including their original data from the given pattern
   * matching result.
   *
   * @param result      pattern matching result
   * @param inputEdges  original data graph edges
   * @param <E> The edge type.
   * @return edges including data
   */
  public static <E extends Edge> DataSet<E> extractEdgesWithData(DataSet<FatVertex> result,
    DataSet<E> inputEdges) {
    return extractEdgeIds(result)
      .join(inputEdges)
      .where(0).equalTo(new Id<>())
      .with(new RightSide<>());
  }
}
