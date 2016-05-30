package org.gradoop.model.impl.operators.matching.isomorphism.naive;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.PatternMatchingBase;
import org.gradoop.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.functions.ElementHasCandidate;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.query.TraversalQueryHandler;
import org.gradoop.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.debug.PrintEmbeddingWithWeldPoint;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.functions.BuildEdgeStep;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.functions.BuildEmbeddingWithTiePoint;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.functions.EdgeHasCandidate;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.functions.UpdateEdgeEmbeddings;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EdgeStep;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EmbeddingWithTiePoint;

/**
 * Algorithm detects subgraphs by traversing the search graph according to a
 * given traversal code which is derived from the query pattern.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 *
 */
public class SubgraphIsomorphism
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends PatternMatchingBase<G, V, E>
  implements UnaryGraphToCollectionOperator<G, V, E> {

  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(SubgraphIsomorphism.class);

  /**
   * Query handler.
   */
  private final TraversalQueryHandler queryHandler;

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
  public SubgraphIsomorphism(String query, boolean attachData) {
    this(query, attachData, null);
  }

  /**
   * Constructor
   *
   * @param query       GDL query graph
   * @param attachData  true, if original data shall be attached to the result
   * @param traverser   Traverser used for the query graph
   */
  public SubgraphIsomorphism(String query, boolean attachData,
    Traverser traverser) {
    super(query, attachData);
    this.queryHandler = (traverser != null) ?
      new TraversalQueryHandler(query, traverser) :
      new TraversalQueryHandler(query);
    this.traversalCode = queryHandler.getTraversalCode();
  }

  @Override
  public GraphCollection<G, V, E> execute(LogicalGraph<G, V, E> graph) {
    if (LOG.isDebugEnabled()) {
      initDebugMappings(graph);
    }

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial initialEmbeddings)
    //--------------------------------------------------------------------------

    DataSet<IdWithCandidates> vertices = PreProcessor.filterVertices(
      graph, query);
    DataSet<TripleWithCandidates> edges = PreProcessor.filterEdges(
      graph, query);

    if (LOG.isDebugEnabled()) {
      vertices = printIdWithCandidates(vertices);
    }

    if (LOG.isDebugEnabled()) {
      edges = printTriplesWithCandidates(edges);
    }

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

    // traverse edges
    DataSet<EdgeStep> edgeSteps = edges
      .filter(new EdgeHasCandidate(traversalCode))
      .map(new BuildEdgeStep(traversalCode));

    DataSet<EmbeddingWithTiePoint> embeddingsWithEdges = embeddings
      .join(edgeSteps)
      .where(1).equalTo(1) // tiePointId == tiePointId
      .with(new UpdateEdgeEmbeddings(traversalCode));

    if (LOG.isDebugEnabled()) {
      embeddingsWithEdges = embeddingsWithEdges
        .map(new PrintEmbeddingWithWeldPoint(true, "embeddingWithE"))
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    }

    // ITERATION FOOTER
    DataSet<EmbeddingWithTiePoint> result = embeddings
      .closeWith(embeddingsWithEdges, embeddingsWithEdges);

    try {
      result.collect();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public String getName() {
    return SubgraphIsomorphism.class.getName();
  }
}
