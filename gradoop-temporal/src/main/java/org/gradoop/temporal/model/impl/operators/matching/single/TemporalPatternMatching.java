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
package org.gradoop.temporal.model.impl.operators.matching.single;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.PairElementWithPropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.debug.PrintIdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.debug.PrintTripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

/**
 * Base class for temporal pattern matching implementations. Analogous to {@link org.gradoop.flink.model.impl.operators.matching.single.PatternMatching}
 *
 * @param <G>  The graph head type.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public abstract class TemporalPatternMatching<
  G extends TemporalGraphHead,
  LG extends TemporalGraph,
  GC extends TemporalGraphCollection>
  implements UnaryBaseGraphToBaseGraphCollectionOperator<LG, GC> {

  /**
   * The property key used to stored the variable mappings inside the GraphHead properties
   */
  public static final transient String VARIABLE_MAPPING_KEY = "__variable_mapping";

  /**
   * GDL based query string
   */
  private final String query;
  /**
   * Logger from the concrete implementation
   */
  private final Logger log;
  /**
   * If true, the original vertex and edge data gets attached to the resulting
   * vertices and edges.
   */
  private final boolean attachData;
  /**
   * Query handler for queries including time data
   */
  private TemporalQueryHandler queryHandler;
  /**
   * Vertex mapping used for debug
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> vertexMapping;
  /**
   * Edge mapping used for debug
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> edgeMapping;
  /**
   * indicates that the query is contradictory
   */
  private boolean contradictoryQuery = false;

  /**
   * Constructor
   *
   * @param query      GDL query
   * @param attachData true, if original data shall be attached to the result
   * @param log        Logger of the concrete implementation
   */
  public TemporalPatternMatching(String query, boolean attachData, Logger log) {
    this(query, attachData, new CNFPostProcessing(), log);
  }

  /**
   * Constructor
   *
   * @param query      GDL query
   * @param attachData true, if original data shall be attached to the result
   * @param cnfProcessor query transformations
   * @param log        Logger of the concrete implementation
   */
  public TemporalPatternMatching(String query, boolean attachData, CNFPostProcessing cnfProcessor,
                                 Logger log) {
    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query = query;

    try {
      queryHandler = new TemporalQueryHandler(query, cnfProcessor);
    } catch (QueryContradictoryException e) {
      contradictoryQuery = true;
    }
    this.attachData = attachData;
    this.log = log;
  }

  @Override
  public GC execute(LG graph) {
    if (log.isDebugEnabled()) {
      initDebugMappings(graph);
    }

    if (contradictoryQuery) {
      return emptyCollection(graph);
    }

    GC result;

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
  protected abstract GC executeForVertex(LG graph);

  /**
   * Computes the result for pattern query graph.
   *
   * @param graph data graph
   * @return result collection
   */
  protected abstract GC executeForPattern(LG graph);

  /**
   * Return an empty result
   *
   * @param graph data graph, possibly needed to create a GC factory in the subclass
   * @return empty result collection
   */
  protected abstract GC emptyCollection(LG graph);

  /**
   * Returns the query handler.
   *
   * @return query handler
   */
  protected TemporalQueryHandler getQueryHandler() {
    return queryHandler;
  }

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
   * @return {@code vertex id -> property value} mapping
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> getVertexMapping() {
    return vertexMapping;
  }

  /**
   * Returns a mapping between edge id and property value used for debug.
   *
   * @return {@code edge id -> property value} mapping
   */
  protected DataSet<Tuple2<GradoopId, PropertyValue>> getEdgeMapping() {
    return edgeMapping;
  }

  //---------------------------------------------------------------------
  // Debugging same as in gradoop-flink
  //---------------------------------------------------------------------

  /**
   * Prints {@link TripleWithCandidates} to debug log.
   *
   * @param edges edge triples with candidates
   * @return edges
   */
  protected DataSet<TripleWithCandidates<GradoopId>> printTriplesWithCandidates(
    DataSet<TripleWithCandidates<GradoopId>> edges) {
    return edges
      .map(new PrintTripleWithCandidates<>())
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
  }

  /**
   * Prints {@link IdWithCandidates} to debug log.
   *
   * @param vertices vertex ids with candidates
   * @return vertices
   */
  protected DataSet<IdWithCandidates<GradoopId>> printIdWithCandidates(
    DataSet<IdWithCandidates<GradoopId>> vertices) {
    return vertices
      .map(new PrintIdWithCandidates<>())
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
  }

  /**
   * Initializes the debug mappings between vertices/edges and their debug id.
   *
   * @param graph data graph
   */
  private void initDebugMappings(LG graph) {
    vertexMapping = graph.getVertices()
      .map(new PairElementWithPropertyValue<>("id"));
    edgeMapping = graph.getEdges()
      .map(new PairElementWithPropertyValue<>("id"));
  }
}
