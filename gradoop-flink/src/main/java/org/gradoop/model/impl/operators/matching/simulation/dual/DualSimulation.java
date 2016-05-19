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

package org.gradoop.model.impl.operators.matching.simulation.dual;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.PairElementWithPropertyValue;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.simulation.dual.debug.PrintDeletion;
import org.gradoop.model.impl.operators.matching.simulation.dual.debug.PrintFatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.debug.PrintMessage;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.BuildFatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.CloneAndReverse;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.CombinedMessages;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.GroupedFatVertices;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.GroupedMessages;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.UpdateVertexState;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.UpdatedFatVertices;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.ValidFatVertices;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.ValidateNeighborhood;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Deletion;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Message;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Vertex-centric Dual-Simulation.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class DualSimulation
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(DualSimulation.class);

  /**
   * Vertex mapping used for debug
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> vertexMapping;

  /**
   * Edge mapping used for debug
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> edgeMapping;

  /**
   * GDL based query string
   */
  private final String query;

  /**
   * If true, the original vertex and edge data gets attached to the resulting
   * vertices and edges.
   */
  private final boolean attachData;

  /**
   * If true, the algorithm uses bulk iteration for the core iteration.
   * Otherwise it uses delta iteration.
   */
  private final boolean useBulkIteration;

  /**
   * Creates a new operator instance.
   *
   * @param query GDL based query
   */
  public DualSimulation(String query) {
    this(query, true, false);
  }

  /**
   * Creates a new operator instance.
   *
   * @param query             GDL based query
   * @param attachData        attach original data to resulting vertices/edges
   * @param useBulkIteration  true to use bulk, false to use delta iteration
   */
  public DualSimulation(String query, boolean attachData,
    boolean useBulkIteration) {

    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query            = query;
    this.attachData       = attachData;
    this.useBulkIteration = useBulkIteration;
  }

  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    if (LOG.isDebugEnabled()) {
      vertexMapping = graph.getVertices()
        .map(new PairElementWithPropertyValue<V>("id"));
      edgeMapping = graph.getEdges()
        .map(new PairElementWithPropertyValue<E>("id"));
    }

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial working set)
    //--------------------------------------------------------------------------

    // TODO: the following is only necessary if diameter(query) > 0

    DataSet<MatchingTriple> triples = filterTriples(graph);
    DataSet<FatVertex> fatVertices = buildInitialWorkingSet(triples);

    //--------------------------------------------------------------------------
    // Dual Simulation
    //--------------------------------------------------------------------------

    // TODO: the following is only necessary if diameter(query) > 1

    DataSet<FatVertex> result = useBulkIteration ?
      simulateBulk(fatVertices) : simulateDelta(fatVertices);

    //--------------------------------------------------------------------------
    // Post-processing (build maximum match graph)
    //--------------------------------------------------------------------------

    return postProcess(graph, result);
  }

  /**
   * Extracts valid triples from the input graph based on the query.
   *
   * @param graph input graph
   * @return triples that have a match in the query graph
   */
  private DataSet<MatchingTriple> filterTriples(LogicalGraph<G, V, E> graph) {
    // filter vertex-edge-vertex triples by query predicates
    return PreProcessor.filterTriplets(graph, query);
  }

  /**
   * Prepares the initial working set for the bulk iteration.
   *
   * @param triples matching triples from the input graph
   * @return data set containing fat vertices
   */
  private DataSet<FatVertex> buildInitialWorkingSet(
    DataSet<MatchingTriple> triples) {
    return triples.flatMap(new CloneAndReverse())
      .groupBy(1) // sourceId
      .combineGroup(new BuildFatVertex(query))
      .groupBy(0) // vertexId
      .reduceGroup(new GroupedFatVertices());
  }

  /**
   * Performs dual simulation using bulkd iteration.
   *
   * @param vertices fat vertices
   * @return remaining fat vertices after dual simulation
   */
  private DataSet<FatVertex> simulateBulk(DataSet<FatVertex> vertices) {

    if (LOG.isDebugEnabled()) {
      vertices = vertices
        .map(new PrintFatVertex(false, "iteration start"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // ITERATION HEAD
    IterativeDataSet<FatVertex> workSet = vertices.iterate(Integer.MAX_VALUE);

    // ITERATION BODY

    // validate neighborhood of each vertex and create deletions
    DataSet<Deletion> deletions = workSet
      .filter(new UpdatedFatVertices())
      .flatMap(new ValidateNeighborhood(query));

    if (LOG.isDebugEnabled()) {
      deletions = deletions
        .map(new PrintDeletion(true, "deletion"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // combine deletions to message
    DataSet<Message> combinedMessages = deletions
      .groupBy(0)
      .combineGroup(new CombinedMessages());

    if (LOG.isDebugEnabled()) {
      combinedMessages = combinedMessages
        .map(new PrintMessage(true, "combined"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // group messages to final message
    DataSet<Message> messages = combinedMessages
      .groupBy(0)
      .reduceGroup(new GroupedMessages());

    if (LOG.isDebugEnabled()) {
      messages = messages
        .map(new PrintMessage(true, "grouped"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // update candidates and build next working set
    DataSet<FatVertex> nextWorkingSet = workSet
      .leftOuterJoin(messages)
      .where(0).equalTo(0) // vertexId == recipientId
      .with(new UpdateVertexState(query))
      .filter(new ValidFatVertices());

    if (LOG.isDebugEnabled()) {
      nextWorkingSet = nextWorkingSet
        .map(new PrintFatVertex(true, "next workset"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // ITERATION FOOTER
    return workSet.closeWith(nextWorkingSet, deletions);
  }

  /**
   * Performs dual simulation using delta iteration.
   *
   * @param vertices fat vertices
   * @return remaining fat vertices after dual simulation
   */
  private DataSet<FatVertex> simulateDelta(DataSet<FatVertex> vertices) {
    // prepare initial working set
    DataSet<Message> initialWorkingSet = vertices
      .flatMap(new ValidateNeighborhood(query))
      .groupBy(0)
      .combineGroup(new CombinedMessages())
      .groupBy(0)
      .reduceGroup(new GroupedMessages());

    if (LOG.isDebugEnabled()) {
      vertices = vertices
        .map(new PrintFatVertex(false, "initial solution set"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);

      initialWorkingSet = initialWorkingSet
        .map(new PrintMessage(false, "initial working set"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // ITERATION HEAD
    DeltaIteration<FatVertex, Message> iteration = vertices
      .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

    // ITERATION BODY

    // get updated vertices
    DataSet<FatVertex> deltas = iteration.getSolutionSet()
      .join(iteration.getWorkset())
      .where(0).equalTo(0)
      .with(new UpdateVertexState(query));

    if (LOG.isDebugEnabled()) {
      deltas = deltas
        .map(new PrintFatVertex(true, "solution set delta"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // prepare new messages for the next round from updates
    DataSet<Message> updates = deltas
      .filter(new ValidFatVertices())
      .flatMap(new ValidateNeighborhood(query))
      .groupBy(0)
      .combineGroup(new CombinedMessages())
      .groupBy(0)
      .reduceGroup(new GroupedMessages());

    if (LOG.isDebugEnabled()) {
      updates = updates
        .map(new PrintMessage(true, "next working set"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // ITERATION FOOTER
    // filter vertices with no candidates after iteration
    return iteration.closeWith(deltas, updates).filter(new ValidFatVertices());
  }

  /**
   * Extracts vertices and edges from the query result and constructs a maximum
   * match graph.
   *
   * @param graph    input graph
   * @param vertices valid vertices after simulation
   * @return maximum match graph
   */
  private LogicalGraph<G, V, E> postProcess(LogicalGraph<G, V, E> graph,
    DataSet<FatVertex> vertices) {
    GradoopFlinkConfig<G, V, E> config = graph.getConfig();

    DataSet<V> matchVertices = attachData ?
      PostProcessor.extractVerticesWithData(vertices, graph.getVertices()) :
      PostProcessor.extractVertices(vertices, config.getVertexFactory());

    DataSet<E> matchEdges = attachData ?
      PostProcessor.extractEdgesWithData(vertices, graph.getEdges()) :
      PostProcessor.extractEdges(vertices, config.getEdgeFactory());

    return LogicalGraph.fromDataSets(matchVertices, matchEdges, config);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DualSimulation.class.getName();
  }
}
