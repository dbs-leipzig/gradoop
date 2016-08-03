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

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromId;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.Superstep;
import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.common.functions.AddGraphElementToNewGraph;
import org.gradoop.flink.model.impl.operators.matching.common.functions.ElementHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.common.functions.ElementsFromEmbedding;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.debug.PrintEdgeStep;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.debug.PrintEmbeddingWithWeldPoint;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.debug.PrintVertexStep;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions.BuildEdgeStep;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions.BuildEmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions.BuildVertexStep;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions.EdgeHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions.UpdateEdgeMappings;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions.UpdateVertexMappings;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions.VertexHasCandidate;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.VertexStep;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES;

/**
 * Algorithm detects subgraphs by traversing the search graph according to a
 * given traversal code which is derived from the query pattern.
 */
public class ExplorativeSubgraphIsomorphism extends PatternMatching implements
  UnaryGraphToCollectionOperator {
  /**
   * Name for broadcast set which contains the superstep id.
   */
  public static final String BC_SUPERSTEP = "bc_superstep";
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(
    ExplorativeSubgraphIsomorphism.class);
  /**
   * Traversal code to process the graph.
   */
  private final TraversalCode traversalCode;
  /**
   * Join strategy used for the join between embeddings and edges
   */
  private final JoinOperatorBase.JoinHint edgeStepJoinStrategy;
  /**
   * Join strategy used for the join between embeddings and vertices
   */
  private final JoinOperatorBase.JoinHint vertexStepJoinStrategy;

  /**
   * Constructor
   *
   * @param query      GDL query graph
   * @param attachData true, if original data shall be attached to the result
   */
  public ExplorativeSubgraphIsomorphism(String query, boolean attachData) {
    this(query, attachData, new DFSTraverser());
  }

  /**
   * Constructor
   *
   * @param query       GDL query graph
   * @param attachData  true, if original data shall be attached to the result
   * @param traverser   Traverser used for the query graph
   */
  public ExplorativeSubgraphIsomorphism(String query, boolean attachData,
    Traverser traverser) {
    this(query, attachData, traverser,
      OPTIMIZER_CHOOSES, OPTIMIZER_CHOOSES);
  }

  /**
   * Constructor
   *
   * @param query                   GDL query graph
   * @param attachData              true, if original data shall be attached
   *                                to the result
   * @param traverser               Traverser used for the query graph
   * @param edgeStepJoinStrategy    Join strategy for edge extension
   * @param vertexStepJoinStrategy  Join strategy for vertex extension
   */
  public ExplorativeSubgraphIsomorphism(String query, boolean attachData,
    Traverser traverser,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy) {
    super(query, attachData, LOG);
    traverser.setQueryHandler(getQueryHandler());
    this.traversalCode          = traverser.traverse();
    this.edgeStepJoinStrategy   = edgeStepJoinStrategy;
    this.vertexStepJoinStrategy = vertexStepJoinStrategy;
  }

  @Override
  protected GraphCollection executeForVertex(
    LogicalGraph graph) {
    GradoopFlinkConfig config = graph.getConfig();
    GraphHeadFactory graphHeadFactory = config.getGraphHeadFactory();
    VertexFactory vertexFactory = config.getVertexFactory();

    DataSet<Vertex> matchingVertices = graph.getVertices()
      .filter(new MatchingVertices<>(getQuery()));

    if (!doAttachData()) {
      matchingVertices = matchingVertices
        .map(new Id<Vertex>())
        .map(new ObjectTo1<GradoopId>())
        .map(new VertexFromId(vertexFactory));
    }

    DataSet<Tuple2<Vertex, GraphHead>> pairs = matchingVertices
      .map(new AddGraphElementToNewGraph<Vertex>(graphHeadFactory))
      .returns(new TupleTypeInfo<Tuple2<Vertex, GraphHead>>(
        TypeExtractor.getForClass(vertexFactory.getType()),
        TypeExtractor.getForClass(graphHeadFactory.getType())));

    return GraphCollection.fromDataSets(
      pairs.map(new Value1Of2<Vertex, GraphHead>()),
      pairs.map(new Value0Of2<Vertex, GraphHead>()),
      config);
  }

  @Override
  protected GraphCollection executeForPattern(
    LogicalGraph graph) {

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial embeddings)
    //--------------------------------------------------------------------------

    DataSet<IdWithCandidates> vertices = PreProcessor.filterVertices(
      graph, getQuery());

    DataSet<TripleWithCandidates> edges = PreProcessor.filterEdges(
      graph, getQuery());

    DataSet<EmbeddingWithTiePoint> initialEmbeddings = vertices
      .filter(new ElementHasCandidate(traversalCode.getStep(0).getFrom()))
      .map(new BuildEmbeddingWithTiePoint(traversalCode,
        getQueryHandler().getVertexCount(), getQueryHandler().getEdgeCount()));

    if (LOG.isDebugEnabled()) {
      initialEmbeddings = initialEmbeddings
        .map(new PrintEmbeddingWithWeldPoint())
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }

    //--------------------------------------------------------------------------
    // Exploration via Traversal
    //--------------------------------------------------------------------------

    DataSet<EmbeddingWithTiePoint> result = explore(
      vertices, edges, initialEmbeddings);


    //--------------------------------------------------------------------------
    // Post-Processing (build Graph Collection from embeddings)
    //--------------------------------------------------------------------------

    DataSet<Element> elements = result
      .<Tuple1<Embedding>>project(0)
      .flatMap(new ElementsFromEmbedding(traversalCode,
        graph.getConfig().getGraphHeadFactory(),
        graph.getConfig().getVertexFactory(),
        graph.getConfig().getEdgeFactory()));

    return doAttachData() ?
      PostProcessor.extractGraphCollectionWithData(elements, graph, true) :
      PostProcessor.extractGraphCollection(elements, graph.getConfig(), true);
  }

  /**
   * Explores the data graph according to the traversal code of this operator.
   *
   * @param vertices          vertex candidates
   * @param edges             edge candidates
   * @param initialEmbeddings initial embeddings which are extended
   * @return final embeddings
   */
  private DataSet<EmbeddingWithTiePoint> explore(
    DataSet<IdWithCandidates> vertices, DataSet<TripleWithCandidates> edges,
    DataSet<EmbeddingWithTiePoint> initialEmbeddings) {
    // ITERATION HEAD
    IterativeDataSet<EmbeddingWithTiePoint> embeddings = initialEmbeddings
      .iterate(traversalCode.getSteps().size());

    // ITERATION BODY

    // get current superstep
    DataSet<Integer> superstep = embeddings
      .first(1)
      .map(new Superstep<EmbeddingWithTiePoint>());

    // traverse to outgoing/incoming edges
    DataSet<EdgeStep> edgeSteps = edges
      .filter(new EdgeHasCandidate(traversalCode))
      .withBroadcastSet(superstep, BC_SUPERSTEP)
      .map(new BuildEdgeStep(traversalCode))
      .withBroadcastSet(superstep, BC_SUPERSTEP);

    if (LOG.isDebugEnabled()) {
      edgeSteps = edgeSteps
        .map(new PrintEdgeStep(true, "post-filter-map-edge"))
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }

    DataSet<EmbeddingWithTiePoint> nextWorkSet = embeddings
      .join(edgeSteps, edgeStepJoinStrategy)
      .where(1).equalTo(1) // tiePointId == tiePointId
      .with(new UpdateEdgeMappings(traversalCode));

    if (LOG.isDebugEnabled()) {
      nextWorkSet = nextWorkSet
        .map(new PrintEmbeddingWithWeldPoint(true, "post-edge-update"))
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }

    // traverse to vertices
    DataSet<VertexStep> vertexSteps = vertices
      .filter(new VertexHasCandidate(traversalCode))
      .withBroadcastSet(superstep, BC_SUPERSTEP)
      .map(new BuildVertexStep());

    if (LOG.isDebugEnabled()) {
      vertexSteps = vertexSteps
        .map(new PrintVertexStep(true, "post-filter-project-vertex"))
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }

    nextWorkSet = nextWorkSet
      .join(vertexSteps, vertexStepJoinStrategy)
      .where(1).equalTo(0) // tiePointId == vertexId
      .with(new UpdateVertexMappings(traversalCode));

    if (LOG.isDebugEnabled()) {
      nextWorkSet = nextWorkSet
        .map(new PrintEmbeddingWithWeldPoint(true, "post-vertex-update"))
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }

    // ITERATION FOOTER
    return embeddings
      .closeWith(nextWorkSet, nextWorkSet);
  }

  @Override
  protected QueryHandler getQueryHandler() {
    return new QueryHandler(getQuery());
  }

  @Override
  public String getName() {
    return ExplorativeSubgraphIsomorphism.class.getName();
  }
}
