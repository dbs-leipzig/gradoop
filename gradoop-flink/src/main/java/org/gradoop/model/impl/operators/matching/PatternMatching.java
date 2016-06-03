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

package org.gradoop.model.impl.operators.matching;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.PairElementWithPropertyValue;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.debug.PrintIdWithCandidates;
import org.gradoop.model.impl.operators.matching.common.debug.PrintTripleWithCandidates;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Base class for pattern matching implementations.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public abstract class PatternMatching
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E> {
  /**
   * Logger from the concrete implementation
   */
  protected final Logger log;
  /**
   * Query handler
   */
  protected final QueryHandler queryHandler;
  /**
   * GDL based query string
   */
  protected final String query;
  /**
   * If true, the original vertex and edge data gets attached to the resulting
   * vertices and edges.
   */
  protected final boolean attachData;
  /**
   * Vertex mapping used for debug
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> vertexMapping;
  /**
   * Edge mapping used for debug
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> edgeMapping;

  /**
   * Constructor
   *
   * @param query       GDL query graph
   * @param attachData  true, if original data shall be attached to the result
   */
  public PatternMatching(String query, QueryHandler queryHandler,
    boolean attachData, Logger log) {
    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query         = query;
    this.queryHandler  = queryHandler;
    this.attachData    = attachData;
    this.log           = log;
  }

  @Override
  public GraphCollection<G, V, E> execute(LogicalGraph<G, V, E> graph) {
    if (log.isDebugEnabled()) {
      initDebugMappings(graph);
    }

    GraphCollection<G, V, E> result;

    if (queryHandler.isSingleVertexGraph()) {
      result = executeForVertex(graph);
    } else {
      result = executeForPattern(graph);
    }

    return result;
  }

  /**
   * Computes the result for single vertex query graphs.
   *
   * @param graph data graph
   * @return result collection
   */
  protected abstract GraphCollection<G, V, E> executeForVertex(
    LogicalGraph<G, V, E> graph);

  /**
   * Computes the result for pattern query graph.
   *
   * @param graph data graph
   * @return result collection
   */
  protected abstract GraphCollection<G, V, E> executeForPattern(
    LogicalGraph<G, V, E> graph);

  /**
   * Initializes the debug mappings between vertices/edges and their debug id.
   *
   * @param graph data graph
   */
  protected void initDebugMappings(LogicalGraph<G, V, E> graph) {
    vertexMapping = graph.getVertices()
      .map(new PairElementWithPropertyValue<V>("id"));
    edgeMapping = graph.getEdges()
      .map(new PairElementWithPropertyValue<E>("id"));
  }

  /**
   * Prints {@link TripleWithCandidates} to debug log.
   *
   * @param edges edge triples with candidates
   * @return edges
   */
  protected DataSet<TripleWithCandidates> printTriplesWithCandidates(
    DataSet<TripleWithCandidates> edges) {
    return edges
      .map(new PrintTripleWithCandidates())
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
  }

  /**
   * Prints {@link IdWithCandidates} to debug log.
   *
   * @param vertices vertex ids with candidates
   * @return vertices
   */
  protected DataSet<IdWithCandidates> printIdWithCandidates(
    DataSet<IdWithCandidates> vertices) {
    return vertices
      .map(new PrintIdWithCandidates())
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
  }
}
