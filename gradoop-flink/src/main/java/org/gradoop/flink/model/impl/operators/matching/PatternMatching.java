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

package org.gradoop.flink.model.impl.operators.matching;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.debug
  .PrintIdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.debug
  .PrintTripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.PairElementWithPropertyValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Base class for pattern matching implementations.
 */
public abstract class PatternMatching implements
  UnaryGraphToCollectionOperator {
  /**
   * GDL based query string
   */
  private final String query;
  /**
   * Logger from the concrete implementation
   */
  private final Logger log;
  /**
   * Query handler
   */
  private final QueryHandler queryHandler;
  /**
   * If true, the original vertex and edge data gets attached to the resulting
   * vertices and edges.
   */
  private final boolean attachData;
  /**
   * Vertex mapping used for debug
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> vertexMapping;
  /**
   * Edge mapping used for debug
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> edgeMapping;

  /**
   * Constructor
   *
   * @param query       GDL query graph
   * @param attachData  true, if original data shall be attached to the result
   * @param log         Logger of the concrete implementation
   */
  public PatternMatching(String query, boolean attachData, Logger log) {
    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query         = query;
    this.queryHandler  = getQueryHandler();
    this.attachData    = attachData;
    this.log           = log;
  }

  @Override
  public GraphCollection execute(LogicalGraph graph) {
    if (log.isDebugEnabled()) {
      initDebugMappings(graph);
    }

    GraphCollection result;

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
  protected abstract GraphCollection executeForVertex(LogicalGraph graph);

  /**
   * Computes the result for pattern query graph.
   *
   * @param graph data graph
   * @return result collection
   */
  protected abstract GraphCollection executeForPattern(LogicalGraph graph);

  /**
   * Returns the query handler used by the concrete implementation.
   *
   * @return query handler
   */
  protected abstract QueryHandler getQueryHandler();

  /**
   * Returns the GDL query processed by this operator instance.
   *
   * @return GDL query
   */
  protected String getQuery() {
    return query;
  }

  /**
   * Determines if the original vertex and edge data shall be attached
   * to the vertices/edges in the resulting subgraphs.
   *
   * @return true, if original data must be attached
   */
  protected boolean doAttachData() {
    return attachData;
  }

  /**
   * Returns a mapping between vertex id and property value used for debug.
   *
   * @return vertex id -> property value mapping
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> getVertexMapping() {
    return vertexMapping;
  }

  /**
   * Returns a mapping between edge id and property value used for debug.
   *
   * @return edge id -> property value mapping
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> getEdgeMapping() {
    return edgeMapping;
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

  /**
   * Initializes the debug mappings between vertices/edges and their debug id.
   *
   * @param graph data graph
   */
  private void initDebugMappings(LogicalGraph graph) {
    vertexMapping = graph.getVertices()
      .map(new PairElementWithPropertyValue<Vertex>("id"));
    edgeMapping = graph.getEdges()
      .map(new PairElementWithPropertyValue<Edge>("id"));
  }
}
