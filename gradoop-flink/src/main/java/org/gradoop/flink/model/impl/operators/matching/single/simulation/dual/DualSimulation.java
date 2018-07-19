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
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromId;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug.PrintDeletion;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug.PrintFatVertex;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug.PrintMessage;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.BuildFatVertex;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.CloneAndReverse;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.CombinedMessages;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.GroupedFatVertices;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.GroupedMessages;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.UpdateVertexState;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.UpdatedFatVertices;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.ValidFatVertices;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.ValidateNeighborhood;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.Deletion;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.Message;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * Vertex-centric Dual-Simulation.
 */
public class DualSimulation extends PatternMatching {

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
   * @param query       GDL based query
   * @param attachData  attach original data to resulting vertices/edges
   * @param useBulk     true to use bulk, false to use delta iteration
   */
  public DualSimulation(String query, boolean attachData, boolean useBulk) {
    super(query, attachData, LOG);
    this.useBulkIteration = useBulk;
  }

  @Override
  protected GraphCollection executeForVertex(
    LogicalGraph graph)  {
    DataSet<Tuple1<GradoopId>> matchingVertexIds = PreProcessor
      .filterVertices(graph, getQuery())
      .project(0);

    LogicalGraphFactory graphFactory = graph.getConfig()
      .getLogicalGraphFactory();
    GraphCollectionFactory collectionFactory = graph.getConfig()
      .getGraphCollectionFactory();

    if (doAttachData()) {
      return collectionFactory.fromGraph(
        graphFactory.fromDataSets(matchingVertexIds
            .join(graph.getVertices())
            .where(0).equalTo(new Id<>())
            .with(new RightSide<>())));
    } else {
      return collectionFactory.fromGraph(
        graphFactory.fromDataSets(matchingVertexIds
            .map(new VertexFromId(graph.getConfig().getVertexFactory()))));
    }
  }

  /**
   * Performs dual simulation based on the given query.
   *
   * @param graph data graph
   * @return match graph
   */
  protected GraphCollection executeForPattern(
    LogicalGraph graph) {
    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial working set)
    //--------------------------------------------------------------------------

    DataSet<TripleWithCandidates<GradoopId>> triples = filterTriples(graph);
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
  private DataSet<TripleWithCandidates<GradoopId>> filterTriples(
    LogicalGraph g) {
    // filter vertex-edge-vertex triples by query predicates
    return PreProcessor.filterTriplets(g, getQuery());
  }

  /**
   * Prepares the initial working set for the bulk iteration.
   *
   * @param triples matching triples from the input graph
   * @return data set containing fat vertices
   */
  private DataSet<FatVertex> buildInitialWorkingSet(
    DataSet<TripleWithCandidates<GradoopId>> triples) {
    return triples.flatMap(new CloneAndReverse())
      .groupBy(1) // sourceId
      .combineGroup(new BuildFatVertex(getQuery()))
      .groupBy(0) // vertexId
      .reduceGroup(new GroupedFatVertices());
  }

  /**
   * Performs dual simulation using bulk iteration.
   *
   * @param vertices fat vertices
   * @return remaining fat vertices after dual simulation
   */
  private DataSet<FatVertex> simulateBulk(DataSet<FatVertex> vertices) {

    vertices = log(vertices, new PrintFatVertex(false, "iteration start"),
      getVertexMapping(), getEdgeMapping());

    // ITERATION HEAD
    IterativeDataSet<FatVertex> workSet = vertices.iterate(Integer.MAX_VALUE);

    // ITERATION BODY

    // validate neighborhood of each vertex and create deletions
    DataSet<Deletion> deletions = workSet
      .filter(new UpdatedFatVertices())
      .flatMap(new ValidateNeighborhood(getQuery()));

    deletions = log(deletions, new PrintDeletion(true, "deletion"),
      getVertexMapping(), getEdgeMapping());

    // combine deletions to message
    DataSet<Message> combinedMessages = deletions
      .groupBy(0)
      .combineGroup(new CombinedMessages());

    combinedMessages = log(combinedMessages, new PrintMessage(true, "combined"),
      getVertexMapping(), getEdgeMapping());

    // group messages to final message
    DataSet<Message> messages = combinedMessages
      .groupBy(0)
      .reduceGroup(new GroupedMessages());

    messages = log(messages, new PrintMessage(true, "grouped"),
      getVertexMapping(), getEdgeMapping());

    // update candidates and build next working set
    DataSet<FatVertex> nextWorkingSet = workSet
      .leftOuterJoin(messages)
      .where(0).equalTo(0) // vertexId == recipientId
      .with(new UpdateVertexState(getQuery()))
      .filter(new ValidFatVertices());

    nextWorkingSet = log(nextWorkingSet,
      new PrintFatVertex(true, "next workset"),
      getVertexMapping(), getEdgeMapping());

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
      .flatMap(new ValidateNeighborhood(getQuery()))
      .groupBy(0)
      .combineGroup(new CombinedMessages())
      .groupBy(0)
      .reduceGroup(new GroupedMessages());

    vertices = log(vertices, new PrintFatVertex(false, "initial solution set"),
      getVertexMapping(), getEdgeMapping());

    initialWorkingSet = log(initialWorkingSet,
      new PrintMessage(false, "initial working set"),
      getVertexMapping(), getEdgeMapping());

    // ITERATION HEAD
    DeltaIteration<FatVertex, Message> iteration = vertices
      .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

    // ITERATION BODY

    // get updated vertices
    DataSet<FatVertex> deltas = iteration.getSolutionSet()
      .join(iteration.getWorkset())
      .where(0).equalTo(0)
      .with(new UpdateVertexState(getQuery()));

    deltas = log(deltas, new PrintFatVertex(true, "solution set delta"),
      getVertexMapping(), getEdgeMapping());

    // prepare new messages for the next round from updates
    DataSet<Message> updates = deltas
      .filter(new ValidFatVertices())
      .flatMap(new ValidateNeighborhood(getQuery()))
      .groupBy(0)
      .combineGroup(new CombinedMessages())
      .groupBy(0)
      .reduceGroup(new GroupedMessages());

    updates = log(updates, new PrintMessage(true, "next working set"),
      getVertexMapping(), getEdgeMapping());

    // ITERATION FOOTER
    // filter vertices with no candidates after iteration
    return iteration.closeWith(deltas, updates).filter(new ValidFatVertices());
  }

  /**
   * Extracts vertices and edges from the query result and constructs a
   * maximum match graph.
   *
   * @param graph    input graph
   * @param vertices valid vertices after simulation
   * @return maximum match graph
   */
  private GraphCollection postProcess(LogicalGraph graph,
    DataSet<FatVertex> vertices) {
    GradoopFlinkConfig config = graph.getConfig();

    DataSet<Vertex> matchVertices = doAttachData() ?
      PostProcessor.extractVerticesWithData(vertices, graph.getVertices()) :
      PostProcessor.extractVertices(vertices, config.getVertexFactory());

    DataSet<Edge> matchEdges = doAttachData() ?
      PostProcessor.extractEdgesWithData(vertices, graph.getEdges()) :
      PostProcessor.extractEdges(vertices, config.getEdgeFactory());

    return config.getGraphCollectionFactory().fromGraph(
      config.getLogicalGraphFactory().fromDataSets(matchVertices, matchEdges));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DualSimulation.class.getName();
  }
}
