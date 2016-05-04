package org.gradoop.model.impl.operators.matching.simulation.dual;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.model.impl.operators.matching.common.debug.GradoopIdToDebugId;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.simulation.dual.debug.PrintDeletion;
import org.gradoop.model.impl.operators.matching.simulation.dual.debug.PrintFatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.debug.PrintMessage;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.BuildFatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.DuplicateTriples;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.GroupFatVertices;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.Messages;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.UpdateVertexState;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.UpdatedFatVertices;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.ValidFatVertices;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.ValidateNeighborhood;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Message;

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
   * Debug
   */
  private static boolean debug = true;
  private DataSet<Tuple2<GradoopId, Integer>> vertexMapping;
  private DataSet<Tuple2<GradoopId, Integer>> edgeMapping;

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
   * Creates a new operator instance.
   *
   *  @param query GDL based query
   *
   */
  public DualSimulation(String query) {
    this(query, true);
  }

  /**
   * Creates a new operator instance.
   *
   * @param query GDL based query
   * @param attachData attach original data to resulting vertices/edges
   */
  public DualSimulation(String query, boolean attachData) {
    Preconditions.checkState(!Strings.isNullOrEmpty(query),
      "Query must not be null or empty");
    this.query      = query;
    this.attachData = attachData;
  }

  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    if (debug) {
      vertexMapping = graph.getVertices().map(new GradoopIdToDebugId<V>("id"));
      edgeMapping = graph.getEdges().map(new GradoopIdToDebugId<E>("id"));
    }

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial working set)
    //--------------------------------------------------------------------------

    // TODO: the following is only necessary if diameter(query) > 0

    DataSet<MatchingTriple> triples = filterMatchingTriples(graph);
    DataSet<FatVertex> fatVertices = buildInitialWorkingSet(triples);

    //--------------------------------------------------------------------------
    // Dual Simulation
    //--------------------------------------------------------------------------

    // TODO: the following is only necessary if diameter(query) > 1

    DataSet<FatVertex> result = simulate(fatVertices);

    //--------------------------------------------------------------------------
    // Post-processing (build maximum match graph)
    //--------------------------------------------------------------------------

    return postProcess(graph, result);
  }

  /**
   * Extracts valid triples from the input graph based on the query.
   *
   * @param graph input graph
   *
   * @return triples that have a match in the query
   */
  private DataSet<MatchingTriple> filterMatchingTriples(LogicalGraph<G, V, E> graph) {
    // filter vertex-edge-vertex triples by query predicates
    return PreProcessor.filterTriplets(graph, query);
  }

  /**
   * Prepares the initial working set for the bulk iteration.
   *
   * @param triples matching triples from the input graph
   *
   * @return data set containing fat vertices
   */
  private DataSet<FatVertex> buildInitialWorkingSet(
    DataSet<MatchingTriple> triples) {
    return triples
      .flatMap(new DuplicateTriples())
      .groupBy(1) // source id
      .combineGroup(new BuildFatVertex(query))
      .groupBy(0) // vertex id
      .reduceGroup(new GroupFatVertices());
  }

  /**
   * Executes the dual simulation process on the vertices.
   *
   * @param vertices vertices and their neighborhood information
   * @return valid triples according to dual simulation
   */
  private DataSet<FatVertex> simulate(DataSet<FatVertex> vertices) {

    // ITERATION HEAD
    IterativeDataSet<FatVertex> workingSet = vertices
      .iterate(Integer.MAX_VALUE);

    // ITERATION BODY

    // validate neighborhood of each vertex and create messages
    DataSet<Message> messages = workingSet
      .map(new PrintFatVertex(true, "working set"))
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING)
      .filter(new UpdatedFatVertices())
      .flatMap(new ValidateNeighborhood(query))
      .map(new PrintDeletion(true, "deletion"))
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING)
      .groupBy(0) // recipient
      .combineGroup(new Messages())
      .map(new PrintMessage(true, "combined"))
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING)
      .groupBy(0) // recipient
      .reduceGroup(new Messages())
      .map(new PrintMessage(true, "grouped"))
      .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
      .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);

    // build next working set
    DataSet<FatVertex> nextWorkingSet = workingSet
      // update candidates
      .leftOuterJoin(messages)
      .where(0).equalTo(0) // vertexId == recipientId
      .with(new UpdateVertexState(query))
      .filter(new ValidFatVertices());

    // ITERATION FOOTER
    return workingSet
      .closeWith(nextWorkingSet, messages);
  }

  /**
   * Extracts vertices and edges from the query result and constructs a maximum
   * match graph.
   *
   * @param graph input graph
   * @param vertices valid vertices after simulation
   * @return maximum match graph
   */
  private LogicalGraph<G, V, E> postProcess(LogicalGraph<G, V, E> graph,
    DataSet<FatVertex> vertices) {
    DataSet<V> matchVertices = attachData ?
      PostProcessor.extractVerticesWithData(vertices, graph.getVertices()) :
      PostProcessor.extractVertices(vertices, graph.getConfig()
        .getVertexFactory());

    DataSet<E> matchEdges = attachData ?
      PostProcessor.extractEdgesWithData(vertices, graph.getEdges()) :
      PostProcessor.extractEdges(vertices, graph.getConfig().getEdgeFactory());

    return LogicalGraph.fromDataSets(matchVertices, matchEdges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DualSimulation.class.getName();
  }
}
