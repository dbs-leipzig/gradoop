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
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.MergedGraphIds;
import org.gradoop.flink.model.impl.functions.utils.IsInstance;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual
  .functions.EdgeTriple;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
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
   * Extracts a {@link GraphCollection} from a set of {@link EPGMElement}.
   *
   * @param epgmElements  EPGM elements
   * @param config        Gradoop Flink config
   * @return Graph collection
   */
  public static GraphCollection extractGraphCollection(
    DataSet<EPGMElement> epgmElements, GradoopFlinkConfig config) {
    return extractGraphCollection(epgmElements, config, true);
  }

  /**
   * Extracts a {@link GraphCollection} from a set of {@link EPGMElement}.
   *
   * @param epgmElements  EPGM elements
   * @param config        Gradoop Flink config
   * @param mayOverlap    elements may be contained in multiple graphs
   * @return Graph collection
   */
  @SuppressWarnings("unchecked")
  public static
  GraphCollection extractGraphCollection(
    DataSet<EPGMElement> epgmElements, GradoopFlinkConfig config,
    boolean mayOverlap) {
    Class<EPGMGraphHead> graphHeadType =
      (Class<EPGMGraphHead>) config.getGraphHeadFactory().getType();
    Class<EPGMVertex> vertexType =
      (Class<EPGMVertex>) config.getVertexFactory().getType();
    Class<EPGMEdge> edgeType =
      (Class<EPGMEdge>) config.getEdgeFactory().getType();
    return GraphCollection.fromDataSets(
      extractGraphHeads(epgmElements, graphHeadType),
      extractVertices(epgmElements, vertexType, mayOverlap),
      extractEdges(epgmElements, edgeType, mayOverlap),
      config
    );
  }

  /**
   * Extracts a {@link GraphCollection} from a set of {@link EPGMElement} and
   * attaches the original data from the input {@link LogicalGraph}.
   *
   * @param epgmElements  EPGM elements
   * @param inputGraph    original input graph
   * @param mayOverlap    true, if elements may be contained in multiple graphs
   * @return Graph collection
   */
  public static GraphCollection extractGraphCollectionWithData(
    DataSet<EPGMElement> epgmElements, LogicalGraph inputGraph,
    boolean mayOverlap) {
    GradoopFlinkConfig config = inputGraph.getConfig();

    // get result collection without data
    GraphCollection collection =
      extractGraphCollection(epgmElements, config, mayOverlap);

    // attach data by joining first and merging the graph head ids
    DataSet<EPGMVertex> newVertices = inputGraph.getVertices()
      .join(collection.getVertices())
      .where(new Id<EPGMVertex>()).equalTo(new Id<EPGMVertex>())
      .with(new MergedGraphIds<EPGMVertex>())
      .withForwardedFieldsFirst("id;label;properties;");

    DataSet<EPGMEdge> newEdges = inputGraph.getEdges()
      .join(collection.getEdges())
      .where(new Id<EPGMEdge>()).equalTo(new Id<EPGMEdge>())
      .with(new MergedGraphIds<EPGMEdge>())
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
   * Filters and casts EPGM graph heads from a given set of {@link EPGMElement}
   *
   * @param epgmElements  EPGM elements
   * @param graphHeadType graph head type
   * @param <G>           EPGM graph head type
   * @return EPGM graph heads
   */
  public static <G extends EPGMGraphHead>
  DataSet<G> extractGraphHeads(DataSet<EPGMElement> epgmElements,
    Class<G> graphHeadType) {
    return epgmElements
      .filter(new IsInstance<EPGMElement, G>(graphHeadType))
      .map(new Cast<EPGMElement, G>(graphHeadType))
      .returns(TypeExtractor.createTypeInfo(graphHeadType));
  }

  /**
   * Initializes EPGM vertices from the given pattern matching result.
   *
   * @param result        pattern matching result
   * @param vertexFactory EPGM vertex factory
   * @return EPGM vertices
   */
  public static <V extends EPGMVertex>
  DataSet<EPGMVertex> extractVertices(DataSet<FatVertex> result,
    EPGMVertexFactory vertexFactory) {
    return extractVertexIds(result).map(new VertexFromId(vertexFactory));
  }

  /**
   * Filters and casts EPGM vertices from a given set of {@link EPGMElement}
   *
   * @param epgmElements  EPGM elements
   * @param vertexType    vertex type
   * @param mayOverlap    vertices may be contained in multiple graphs
   * @return EPGM vertices
   */
  public static DataSet<EPGMVertex> extractVertices(DataSet<EPGMElement> epgmElements,
    Class<EPGMVertex> vertexType, boolean mayOverlap) {
    DataSet<EPGMVertex> result = epgmElements
      .filter(new IsInstance<EPGMElement, EPGMVertex>(vertexType))
      .map(new Cast<EPGMElement, EPGMVertex>(vertexType))
      .returns(TypeExtractor.createTypeInfo(vertexType));
    return mayOverlap ? result
      .groupBy(new Id<EPGMVertex>())
      .combineGroup(new MergedGraphIds<EPGMVertex>())
      .groupBy(new Id<EPGMVertex>())
      .reduceGroup(new MergedGraphIds<EPGMVertex>()) : result;
  }

  /**
   * Initializes EPGM edges from the given pattern matching result.
   *
   * @param result      pattern matching result
   * @param edgeFactory EPGM edge factory
   * @return EPGM edges
   */
  public static DataSet<EPGMEdge> extractEdges(DataSet<FatVertex> result,
    EPGMEdgeFactory edgeFactory) {
    return extractEdgeIds(result).map(new EdgeFromIds(edgeFactory));
  }

  /**
   * Filters and casts EPGM edges from a given set of {@link EPGMElement}
   *
   * @param epgmElements  EPGM elements
   * @param edgeType      edge type
   * @param mayOverlap    edges may be contained in multiple graphs
   * @return EPGM edges
   */
  public static DataSet<EPGMEdge> extractEdges(DataSet<EPGMElement> epgmElements,
    Class<EPGMEdge> edgeType, boolean mayOverlap) {
    DataSet<EPGMEdge> result = epgmElements
      .filter(new IsInstance<EPGMElement, EPGMEdge>(edgeType))
      .map(new Cast<EPGMElement, EPGMEdge>(edgeType))
      .returns(TypeExtractor.createTypeInfo(edgeType));

    return mayOverlap ? result
      .groupBy(new Id<EPGMEdge>())
      .combineGroup(new MergedGraphIds<EPGMEdge>()).groupBy(new Id<EPGMEdge>())
      .reduceGroup(new MergedGraphIds<EPGMEdge>()) : result;
  }

  /**
   * Initializes EPGM vertices including their original data from the given
   * pattern matching result.
   *
   * @param result        pattern matching result
   * @param inputVertices original data graph vertices
   * @return EPGM vertices including data
   */
  public static DataSet<EPGMVertex> extractVerticesWithData(DataSet<FatVertex> result,
    DataSet<EPGMVertex> inputVertices) {
    return extractVertexIds(result)
      .join(inputVertices)
      .where(0).equalTo(new Id<EPGMVertex>())
      .with(new RightSide<Tuple1<GradoopId>, EPGMVertex>());
  }

  /**
   * Initializes EPGM edges including their original data from the given pattern
   * matching result.
   *
   * @param result      pattern matching result
   * @param inputEdges  original data graph edges
   * @return EPGM edges including data
   */
  public static DataSet<EPGMEdge> extractEdgesWithData(
    DataSet<FatVertex> result, DataSet<EPGMEdge> inputEdges) {
    return extractEdgeIds(result)
      .join(inputEdges)
      .where(0).equalTo(new Id<EPGMEdge>())
      .with(new RightSide<Tuple3<GradoopId, GradoopId, GradoopId>, EPGMEdge>());
  }
}
