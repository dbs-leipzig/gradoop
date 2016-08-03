/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.MergedGraphIds;
import org.gradoop.flink.model.impl.functions.utils.IsInstance;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.functions.EdgeTriple;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.EdgeFromIds;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromId;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.functions.utils.Cast;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.FatVertex;

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
    return GraphCollection.fromDataSets(
      extractGraphHeads(elements, graphHeadType),
      extractVertices(elements, vertexType, mayOverlap),
      extractEdges(elements, edgeType, mayOverlap),
      config
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
    GraphCollection collection =
      extractGraphCollection(elements, config, mayOverlap);

    // attach data by joining first and merging the graph head ids
    DataSet<Vertex> newVertices = inputGraph.getVertices()
      .join(collection.getVertices())
      .where(new Id<Vertex>()).equalTo(new Id<Vertex>())
      .with(new MergedGraphIds<Vertex>())
      .withForwardedFieldsFirst("id;label;properties;");

    DataSet<Edge> newEdges = inputGraph.getEdges()
      .join(collection.getEdges())
      .where(new Id<Edge>()).equalTo(new Id<Edge>())
      .with(new MergedGraphIds<Edge>())
      .withForwardedFieldsFirst("id;label;properties");

    return GraphCollection.fromDataSets(
      collection.getGraphHeads(), newVertices, newEdges, config);
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
      .filter(new IsInstance<Element, GraphHead>(graphHeadType))
      .map(new Cast<Element, GraphHead>(graphHeadType))
      .returns(TypeExtractor.createTypeInfo(graphHeadType));
  }

  /**
   * Initializes EPGM vertices from the given pattern matching result.
   *
   * @param result        pattern matching result
   * @param vertexFactory EPGM vertex factory
   * @return EPGM vertices
   */
  public static DataSet<Vertex> extractVertices(DataSet<FatVertex> result,
    VertexFactory vertexFactory) {
    return extractVertexIds(result).map(new VertexFromId(vertexFactory));
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
      .filter(new IsInstance<Element, Vertex>(vertexType))
      .map(new Cast<Element, Vertex>(vertexType))
      .returns(TypeExtractor.createTypeInfo(vertexType));
    return mayOverlap ? result
      .groupBy(new Id<Vertex>())
      .combineGroup(new MergedGraphIds<Vertex>())
      .groupBy(new Id<Vertex>())
      .reduceGroup(new MergedGraphIds<Vertex>()) : result;
  }

  /**
   * Initializes EPGM edges from the given pattern matching result.
   *
   * @param result      pattern matching result
   * @param edgeFactory EPGM edge factory
   * @return EPGM edges
   */
  public static DataSet<Edge> extractEdges(DataSet<FatVertex> result,
    EdgeFactory edgeFactory) {
    return extractEdgeIds(result).map(new EdgeFromIds(edgeFactory));
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
      .filter(new IsInstance<Element, Edge>(edgeType))
      .map(new Cast<Element, Edge>(edgeType))
      .returns(TypeExtractor.createTypeInfo(edgeType));

    return mayOverlap ? result
      .groupBy(new Id<Edge>())
      .combineGroup(new MergedGraphIds<Edge>()).groupBy(new Id<Edge>())
      .reduceGroup(new MergedGraphIds<Edge>()) : result;
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
      .where(0).equalTo(new Id<Vertex>())
      .with(new RightSide<Tuple1<GradoopId>, Vertex>());
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
      .where(0).equalTo(new Id<Edge>())
      .with(new RightSide<Tuple3<GradoopId, GradoopId, GradoopId>, Edge>());
  }
}
