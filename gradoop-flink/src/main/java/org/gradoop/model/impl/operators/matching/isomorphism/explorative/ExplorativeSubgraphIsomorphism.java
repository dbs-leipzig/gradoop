package org.gradoop.model.impl.operators.matching.isomorphism.explorative;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.VertexFromId;
import org.gradoop.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.model.impl.functions.tuple.Value0Of2;
import org.gradoop.model.impl.functions.tuple.Value1Of2;
import org.gradoop.model.impl.functions.utils.SuperStep;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.PatternMatching;
import org.gradoop.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.functions.ElementHasCandidate;
import org.gradoop.model.impl.operators.matching.common.functions.ElementsFromEmbedding;
import org.gradoop.model.impl.operators.matching.common.functions.AddGraphElementToNewGraph;
import org.gradoop.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.query.TraversalQueryHandler;
import org.gradoop.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.debug.PrintEdgeStep;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.debug.PrintEmbeddingWithWeldPoint;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.debug.PrintVertexStep;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.functions.*;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.EdgeStep;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.VertexStep;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.utils.Constants;
import org.gradoop.util.GradoopFlinkConfig;

import static org.gradoop.model.impl.operators.matching.common.PostProcessor.extractGraphCollection;
import static org.gradoop.model.impl.operators.matching.common.PostProcessor.extractGraphCollectionWithData;

/**
 * Algorithm detects subgraphs by traversing the search graph according to a
 * given traversal code which is derived from the query pattern.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 *
 */
public class ExplorativeSubgraphIsomorphism
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends PatternMatching<G, V, E>
  implements UnaryGraphToCollectionOperator<G, V, E> {

  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(ExplorativeSubgraphIsomorphism.class);

  /**
   * Traversal code to process the graph.
   */
  private final TraversalCode traversalCode;

  /**
   * Constructor
   *
   * @param query      GDL query graph
   * @param attachData true, if original data shall be attached to the result
   */
  public ExplorativeSubgraphIsomorphism(String query, boolean attachData) {
    this(query, attachData, null);
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
    super(query, (traverser != null) ?
      new TraversalQueryHandler(query, traverser) :
      new TraversalQueryHandler(query), attachData, LOG);

    this.traversalCode = ((TraversalQueryHandler) queryHandler)
      .getTraversalCode();
  }

  @Override
  protected GraphCollection<G, V, E> executeForVertex(
    LogicalGraph<G, V, E> graph) {
    GradoopFlinkConfig<G, V, E> config = graph.getConfig();
    EPGMGraphHeadFactory<G> graphHeadFactory = config.getGraphHeadFactory();
    EPGMVertexFactory<V> vertexFactory = config.getVertexFactory();

    DataSet<V> matchingVertices = graph.getVertices()
      .filter(new MatchingVertices<V>(query));

    if (!attachData) {
      matchingVertices = matchingVertices
        .map(new Id<V>())
        .map(new ObjectTo1<GradoopId>())
        .map(new VertexFromId<>(vertexFactory));
    }

    DataSet<Tuple2<V, G>> pairs = matchingVertices
      .map(new AddGraphElementToNewGraph<V, G>(graphHeadFactory))
      .returns(new TupleTypeInfo<Tuple2<V, G>>(
        TypeExtractor.getForClass(vertexFactory.getType()),
        TypeExtractor.getForClass(graphHeadFactory.getType())));

    return GraphCollection.fromDataSets(
      pairs.map(new Value1Of2<V, G>()),
      pairs.map(new Value0Of2<V, G>()),
      config);
  }

  @Override
  protected GraphCollection<G, V, E> executeForPattern(
    LogicalGraph<G, V, E> graph) {

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial initialEmbeddings)
    //--------------------------------------------------------------------------

    DataSet<IdWithCandidates> vertices = PreProcessor.filterVertices(
      graph, query);

    DataSet<TripleWithCandidates> edges = PreProcessor.filterEdges(
      graph, query);

    DataSet<EmbeddingWithTiePoint> initialEmbeddings = vertices
      .filter(new ElementHasCandidate(traversalCode.getStep(0).getFrom()))
      .map(new BuildEmbeddingWithTiePoint(traversalCode,
        queryHandler.getVertexCount(), queryHandler.getEdgeCount()));

    if (LOG.isDebugEnabled()) {
      initialEmbeddings = initialEmbeddings
        .map(new PrintEmbeddingWithWeldPoint())
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    //--------------------------------------------------------------------------
    // Traversal
    //--------------------------------------------------------------------------

    // ITERATION HEAD
    IterativeDataSet<EmbeddingWithTiePoint> embeddings = initialEmbeddings
      .iterate(traversalCode.getSteps().size());

    // ITERATION BODY

    // get current superstep
    DataSet<Integer> superStep = embeddings
      .first(1)
      .map(new SuperStep());

    // traverse to outgoing/incoming edges
    DataSet<EdgeStep> edgeSteps = edges
      .filter(new EdgeHasCandidate(traversalCode))
      .withBroadcastSet(superStep, Constants.BC_SUPERSTEP)
      .map(new BuildEdgeStep(traversalCode))
      .withBroadcastSet(superStep, Constants.BC_SUPERSTEP);

    if (LOG.isDebugEnabled()) {
      edgeSteps = edgeSteps
        .map(new PrintEdgeStep(true, "post-filter-map-edge"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    DataSet<EmbeddingWithTiePoint> nextWorkSet = embeddings
      .join(edgeSteps)
      .where(1).equalTo(1) // tiePointId == tiePointId
      .with(new UpdateEdgeMappings(traversalCode));

    if (LOG.isDebugEnabled()) {
      nextWorkSet = nextWorkSet
        .map(new PrintEmbeddingWithWeldPoint(true, "post-edge-update"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // traverse to vertices
    DataSet<VertexStep> vertexSteps = vertices
      .filter(new VertexHasCandidate(traversalCode))
      .withBroadcastSet(superStep, Constants.BC_SUPERSTEP)
      .map(new BuildVertexStep());

    if (LOG.isDebugEnabled()) {
      vertexSteps = vertexSteps
        .map(new PrintVertexStep(true, "post-filter-project-vertex"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    nextWorkSet = nextWorkSet
      .join(vertexSteps)
      .where(1).equalTo(0) // tiePointId == vertexId
      .with(new UpdateVertexMappings(traversalCode));

    if (LOG.isDebugEnabled()) {
      nextWorkSet = nextWorkSet
        .map(new PrintEmbeddingWithWeldPoint(true, "post-vertex-update"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // ITERATION FOOTER
    DataSet<EmbeddingWithTiePoint> result = embeddings
      .closeWith(nextWorkSet, nextWorkSet);

    //--------------------------------------------------------------------------
    // Post-Processing (Build Graph Collection from Embeddings)
    //--------------------------------------------------------------------------

    DataSet<EPGMElement> epgmElements = result
      .<Tuple1<Embedding>>project(0)
      .flatMap(new ElementsFromEmbedding<>(traversalCode,
        graph.getConfig().getGraphHeadFactory(),
        graph.getConfig().getVertexFactory(),
        graph.getConfig().getEdgeFactory()));

    return attachData ?
      extractGraphCollectionWithData(epgmElements, graph, true) :
      extractGraphCollection(epgmElements, graph.getConfig(), true);
  }

  @Override
  public String getName() {
    return ExplorativeSubgraphIsomorphism.class.getName();
  }
}
