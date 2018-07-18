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
package org.gradoop.flink.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.EdgeFromIds;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.MergedGraphIds;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromId;
import org.gradoop.flink.model.impl.functions.utils.Cast;
import org.gradoop.flink.model.impl.functions.utils.IsInstance;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.EdgeTriple;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Provides methods for post-processing query results.
 */
public class PostProcessor {

  /**
   * Extracts a {@link GraphCollection} from a set of {@link Element}.
   *
   * @param elements  EPGM elements
   * @param config    Gradoop Flink config
   * @return Graph collection
   */
  public static GraphCollection extractGraphCollection(
    DataSet<Element> elements, GradoopFlinkConfig config) {
    return extractGraphCollection(elements, config, true);
  }

  /**
   * Extracts a {@link GraphCollection} from a set of {@link Element}.
   *
   * @param elements  EPGM elements
   * @param config        Gradoop Flink config
   * @param mayOverlap    elements may be contained in multiple graphs
   * @return Graph collection
   */
  @SuppressWarnings("unchecked")
  public static GraphCollection extractGraphCollection(
    DataSet<Element> elements, GradoopFlinkConfig config, boolean mayOverlap) {

    Class<GraphHead> graphHeadType = config.getGraphHeadFactory().getType();
    Class<Vertex> vertexType = config.getVertexFactory().getType();
    Class<Edge> edgeType = config.getEdgeFactory().getType();
    return config.getGraphCollectionFactory().fromDataSets(
      extractGraphHeads(elements, graphHeadType),
      extractVertices(elements, vertexType, mayOverlap),
      extractEdges(elements, edgeType, mayOverlap)
    );
  }

  /**
   * Extracts a {@link GraphCollection} from a set of {@link Element} and
   * attaches the original data from the input {@link LogicalGraph}.
   *
   * @param elements      EPGM elements
   * @param inputGraph    original input graph
   * @param mayOverlap    true, if elements may be contained in multiple graphs
   * @return Graph collection
   */
  public static GraphCollection extractGraphCollectionWithData(
    DataSet<Element> elements, LogicalGraph inputGraph, boolean mayOverlap) {

    GradoopFlinkConfig config = inputGraph.getConfig();

    // get result collection without data
    GraphCollection collection = extractGraphCollection(elements, config, mayOverlap);

    // attach data by joining first and merging the graph head ids
    DataSet<Vertex> newVertices = inputGraph.getVertices()
      .rightOuterJoin(collection.getVertices())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new MergedGraphIds<>())
      .withForwardedFieldsFirst("id;label;properties;");


    DataSet<Edge> newEdges = inputGraph.getEdges()
      .rightOuterJoin(collection.getEdges())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new MergedGraphIds<>())
      .withForwardedFieldsFirst("id;label;properties");

    return config.getGraphCollectionFactory().fromDataSets(
      collection.getGraphHeads(), newVertices, newEdges);
  }
  /**
   * Extracts vertex ids from the given pattern matching result.
   *
   * @param result pattern matching result
   * @return dataset with (vertexId) tuples
   */
  public static DataSet<Tuple1<GradoopId>> extractVertexIds(
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
  public static DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> extractEdgeIds(
    DataSet<FatVertex> result) {
    return result.flatMap(new EdgeTriple());
  }

  /**
   * Filters and casts EPGM graph heads from a given set of {@link Element}
   *
   * @param elements      EPGM elements
   * @param graphHeadType graph head type
   * @return EPGM graph heads
   */
  public static DataSet<GraphHead> extractGraphHeads(DataSet<Element> elements,
    Class<GraphHead> graphHeadType) {
    return elements
      .filter(new IsInstance<>(graphHeadType))
      .map(new Cast<>(graphHeadType))
      .returns(TypeExtractor.createTypeInfo(graphHeadType));
  }

  /**
   * Initializes EPGM vertices from the given pattern matching result.
   *
   * @param result        pattern matching result
   * @param epgmVertexFactory EPGM vertex factory
   * @return EPGM vertices
   */
  public static DataSet<Vertex> extractVertices(DataSet<FatVertex> result,
                                                EPGMVertexFactory<Vertex> epgmVertexFactory) {
    return extractVertexIds(result).map(new VertexFromId(epgmVertexFactory));
  }

  /**
   * Filters and casts EPGM vertices from a given set of {@link Element}
   *
   * @param elements  EPGM elements
   * @param vertexType    vertex type
   * @param mayOverlap    vertices may be contained in multiple graphs
   * @return EPGM vertices
   */
  public static DataSet<Vertex> extractVertices(DataSet<Element> elements,
    Class<Vertex> vertexType, boolean mayOverlap) {
    DataSet<Vertex> result = elements
      .filter(new IsInstance<>(vertexType))
      .map(new Cast<>(vertexType))
      .returns(TypeExtractor.createTypeInfo(vertexType));
    return mayOverlap ? result
      .groupBy(new Id<>())
      .combineGroup(new MergedGraphIds<>())
      .groupBy(new Id<>())
      .reduceGroup(new MergedGraphIds<>()) : result;
  }

  /**
   * Initializes EPGM edges from the given pattern matching result.
   *
   * @param result      pattern matching result
   * @param epgmEdgeFactory EPGM edge factory
   * @return EPGM edges
   */
  public static DataSet<Edge> extractEdges(DataSet<FatVertex> result,
    EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    return extractEdgeIds(result).map(new EdgeFromIds(epgmEdgeFactory));
  }

  /**
   * Filters and casts EPGM edges from a given set of {@link Element}
   *
   * @param elements      EPGM elements
   * @param edgeType      edge type
   * @param mayOverlap    edges may be contained in multiple graphs
   * @return EPGM edges
   */
  public static DataSet<Edge> extractEdges(DataSet<Element> elements,
    Class<Edge> edgeType, boolean mayOverlap) {
    DataSet<Edge> result = elements
      .filter(new IsInstance<>(edgeType))
      .map(new Cast<>(edgeType))
      .returns(TypeExtractor.createTypeInfo(edgeType));

    return mayOverlap ? result
      .groupBy(new Id<>())
      .combineGroup(new MergedGraphIds<>()).groupBy(new Id<>())
      .reduceGroup(new MergedGraphIds<>()) : result;
  }

  /**
   * Initializes EPGM vertices including their original data from the given
   * pattern matching result.
   *
   * @param result        pattern matching result
   * @param inputVertices original data graph vertices
   * @return EPGM vertices including data
   */
  public static DataSet<Vertex> extractVerticesWithData(
    DataSet<FatVertex> result, DataSet<Vertex> inputVertices) {
    return extractVertexIds(result)
      .join(inputVertices)
      .where(0).equalTo(new Id<>())
      .with(new RightSide<>());
  }

  /**
   * Initializes EPGM edges including their original data from the given pattern
   * matching result.
   *
   * @param result      pattern matching result
   * @param inputEdges  original data graph edges
   * @return EPGM edges including data
   */
  public static DataSet<Edge> extractEdgesWithData(DataSet<FatVertex> result,
    DataSet<Edge> inputEdges) {
    return extractEdgeIds(result)
      .join(inputEdges)
      .where(0).equalTo(new Id<>())
      .with(new RightSide<>());
  }
}
