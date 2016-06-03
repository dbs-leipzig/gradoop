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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.VertexFromId;
import org.gradoop.model.impl.functions.utils.RightSide;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.PatternMatching;
import org.gradoop.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
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
  extends PatternMatching<G, V, E> {

  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(DualSimulation.class);

  /**
   * If true, the algorithm uses bulk iteration for the core iteration.
   * Otherwise it uses delta iteration.
   */
  private final boolean useBulkIteration;

  /**
   * Creates a new operator instance.
   *
   * @param query             GDL based query
   * @param attachData        attach original data to resulting vertices/edges
   * @param useBulkIteration  true to use bulk, false to use delta iteration
   */
  public DualSimulation(String query, boolean attachData,
    boolean useBulkIteration) {
    super(query, new QueryHandler(query), attachData, LOG);
    this.useBulkIteration = useBulkIteration;
  }

  @Override
  protected GraphCollection<G, V, E> executeForVertex(
    LogicalGraph<G, V, E> graph)  {
    DataSet<Tuple1<GradoopId>> matchingVertexIds = PreProcessor
      .filterVertices(graph, query)
      .project(0);

    if (attachData) {
      return GraphCollection.fromGraph(
        LogicalGraph.fromDataSets(matchingVertexIds
            .join(graph.getVertices())
            .where(0).equalTo(new Id<V>())
            .with(new RightSide<Tuple1<GradoopId>, V>()),
          graph.getConfig()
        ));
    } else {
      return GraphCollection.fromGraph(
        LogicalGraph.fromDataSets(matchingVertexIds
            .map(new VertexFromId<>(graph.getConfig().getVertexFactory())),
          graph.getConfig()
        ));
    }
  }

  /**
   * Performs dual simulation based on the given query.
   *
   * @param graph data graph
   * @return match graph
   */
  protected GraphCollection<G, V, E> executeForPattern(
    LogicalGraph<G, V, E> graph) {
    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial working set)
    //--------------------------------------------------------------------------

    DataSet<TripleWithCandidates> triples = filterTriples(graph);
    DataSet<FatVertex> fatVertices = buildInitialWorkingSet(triples);

    //--------------------------------------------------------------------------
    // Dual Simulation
    //--------------------------------------------------------------------------

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
   * @param g input graph
   * @return triples that have a match in the query graph
   */
  private DataSet<TripleWithCandidates> filterTriples(LogicalGraph<G, V, E> g) {
    // filter vertex-edge-vertex triples by query predicates
    return PreProcessor.filterTriplets(g, query);
  }

  /**
   * Prepares the initial working set for the bulk iteration.
   *
   * @param triples matching triples from the input graph
   * @return data set containing fat vertices
   */
  private DataSet<FatVertex> buildInitialWorkingSet(
    DataSet<TripleWithCandidates> triples) {
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
  private GraphCollection<G, V, E> postProcess(LogicalGraph<G, V, E> graph,
    DataSet<FatVertex> vertices) {
    GradoopFlinkConfig<G, V, E> config = graph.getConfig();

    DataSet<V> matchVertices = attachData ?
      PostProcessor.extractVerticesWithData(vertices, graph.getVertices()) :
      PostProcessor.extractVertices(vertices, config.getVertexFactory());

    DataSet<E> matchEdges = attachData ?
      PostProcessor.extractEdgesWithData(vertices, graph.getEdges()) :
      PostProcessor.extractEdges(vertices, config.getEdgeFactory());

    return GraphCollection.fromGraph(
      LogicalGraph.fromDataSets(matchVertices, matchEdges, config));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DualSimulation.class.getName();
  }
}
