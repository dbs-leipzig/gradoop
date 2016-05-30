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

package org.gradoop.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.epgm.EdgeFromIds;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.MergedGraphIds;
import org.gradoop.model.impl.functions.epgm.VertexFromId;
import org.gradoop.model.impl.functions.utils.RightSide;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.functions.Cast;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.functions.IsInstance;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.EdgeTriple;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;


import org.gradoop.util.GradoopFlinkConfig;

/**
 * Provides methods for post-processing query results.
 */
public class PostProcessor {

  /**
   * Extracts a {@link GraphCollection} from a set of {@link EPGMElement}.
   *
   * @param epgmElements  EPGM elements
   * @param config        Gradoop Flink config
   * @param <G>           EPGM graph head type
   * @param <V>           EPGM vertex type
   * @param <E>           EPGM edge type
   * @return Graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GraphCollection<G, V, E> extractGraphCollection(
    DataSet<EPGMElement> epgmElements, GradoopFlinkConfig<G, V, E> config,
    boolean mayOverlap) {
    return GraphCollection.fromDataSets(
      extractGraphHeads(epgmElements, config.getGraphHeadFactory().getType()),
      extractVertices(epgmElements, config.getVertexFactory().getType()),
      extractEdges(epgmElements, config.getEdgeFactory().getType()),
      config
    );
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
   * @param <V>           EPGM vertex type
   * @return EPGM vertices
   */
  public static <V extends EPGMVertex>
  DataSet<V> extractVertices(DataSet<FatVertex> result,
    EPGMVertexFactory<V> vertexFactory) {
    return extractVertexIds(result).map(new VertexFromId<>(vertexFactory));
  }

  /**
   * Filters and casts EPGM vertices from a given set of {@link EPGMElement}
   *
   * @param epgmElements  EPGM elements
   * @param vertexType    vertex type
   * @param <V>           EPGM vertex type
   * @return EPGM vertices
   */
  public static <V extends EPGMVertex>
  DataSet<V> extractVertices(DataSet<EPGMElement> epgmElements,
    Class<V> vertexType) {
    return epgmElements
      .filter(new IsInstance<EPGMElement, V>(vertexType))
      .map(new Cast<EPGMElement, V>(vertexType))
      .returns(TypeExtractor.createTypeInfo(vertexType))
      .groupBy(new Id<V>())
      .combineGroup(new MergedGraphIds<V>())
      .groupBy(new Id<V>())
      .reduceGroup(new MergedGraphIds<V>());
  }

  /**
   * Initializes EPGM edges from the given pattern matching result.
   *
   * @param result      pattern matching result
   * @param edgeFactory EPGM edge factory
   * @param <E>         EPGM edge type
   * @return EPGM edges
   */
  public static <E extends EPGMEdge>
  DataSet<E> extractEdges(DataSet<FatVertex> result,
    EPGMEdgeFactory<E> edgeFactory) {
    return extractEdgeIds(result).map(new EdgeFromIds<>(edgeFactory));
  }

  /**
   * Filters and casts EPGM edges from a given set of {@link EPGMElement}
   *
   * @param epgmElements  EPGM elements
   * @param edgeType      edge type
   * @param <E>           EPGM edge type
   * @return EPGM edges
   */
  public static <E extends EPGMEdge>
  DataSet<E> extractEdges(DataSet<EPGMElement> epgmElements,
    Class<E> edgeType) {
    return epgmElements
      .filter(new IsInstance<EPGMElement, E>(edgeType))
      .map(new Cast<EPGMElement, E>(edgeType))
      .returns(TypeExtractor.createTypeInfo(edgeType))
      .groupBy(new Id<E>())
      .combineGroup(new MergedGraphIds<E>())
      .groupBy(new Id<E>())
      .reduceGroup(new MergedGraphIds<E>());
  }

  /**
   * Initializes EPGM vertices including their original data from the given
   * pattern matching result.
   *
   * @param result        pattern matching result
   * @param inputVertices original data graph vertices
   * @param <V>           EPGM vertex type
   * @return EPGM vertices including data
   */
  public static <V extends EPGMVertex>
  DataSet<V> extractVerticesWithData(DataSet<FatVertex> result,
    DataSet<V> inputVertices) {
    return extractVertexIds(result)
      .join(inputVertices)
      .where(0).equalTo(new Id<V>())
      .with(new RightSide<Tuple1<GradoopId>, V>());
  }

  /**
   * Initializes EPGM edges including their original data from the given pattern
   * matching result.
   *
   * @param result      pattern matching result
   * @param inputEdges  original data graph edges
   * @param <E>         EPGM edge type
   * @return EPGM edges including data
   */
  public static <E extends EPGMEdge>
  DataSet<E> extractEdgesWithData(DataSet<FatVertex> result,
    DataSet<E> inputEdges) {
    return extractEdgeIds(result)
      .join(inputEdges)
      .where(0).equalTo(new Id<E>())
      .with(new RightSide<Tuple3<GradoopId, GradoopId, GradoopId>, E>());
  }
}
