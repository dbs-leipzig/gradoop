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

package org.gradoop.flink.model.impl.operators.matching.preserving.explorative;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
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
import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.functions.AddGraphElementToNewGraph;
import org.gradoop.flink.model.impl.operators.matching.common.functions.ElementsFromEmbedding;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES;

/**
 * Algorithm detects subgraphs by traversing the search graph according to a
 * given traversal code which is derived from the query pattern.
 */
public class ExplorativePatternMatching extends PatternMatching
  implements UnaryGraphToCollectionOperator {
  /**
   * Name for broadcast set which contains the superstep id.
   */
  public static final String BC_SUPERSTEP = "bc_superstep";
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(
    ExplorativePatternMatching.class);
  /**
   * Holds information on how to traverse the graph.
   */
  private final Traverser traverser;
  /**
   * Join strategy used for the join between embeddings and edges
   */
  private final JoinOperatorBase.JoinHint edgeStepJoinStrategy;
  /**
   * Join strategy used for the join between embeddings and vertices
   */
  private final JoinOperatorBase.JoinHint vertexStepJoinStrategy;

  /**
   * Match strategy used for pattern matching
   */
  private final MatchStrategy matchStrategy;



  /**
   * Create new operator instance
   *
   * @param query      GDL query graph
   * @param attachData true, if original data shall be attached to the result
   * @param matchStrategy select Subgraph Isomorphism or Homomorphism
   */
  public ExplorativePatternMatching(String query, boolean attachData,
    MatchStrategy matchStrategy) {
    this(query, attachData, matchStrategy, new DFSTraverser());
  }

  /**
   * Create new operator instance
   *
   * @param query       GDL query graph
   * @param attachData  true, if original data shall be attached to the result
   * @param matchStrategy select Subgraph Isomorphism or Homomorphism
   * @param traverser   Traverser used for the query graph
   */
  public ExplorativePatternMatching(String query, boolean attachData,
    MatchStrategy matchStrategy,
    Traverser traverser) {
    this(query, attachData, matchStrategy, traverser,
      OPTIMIZER_CHOOSES, OPTIMIZER_CHOOSES);
  }

  /**
   * Create new operator instance
   *
   * @param query                   GDL query graph
   * @param attachData              true, if original data shall be attached
   *                                to the result
   * @param matchStrategy           select Subgraph Isomorphism or Homomorphism
   * @param traverser               Traverser used for the query graph
   * @param edgeStepJoinStrategy    Join strategy for edge extension
   * @param vertexStepJoinStrategy  Join strategy for vertex extension
   */
  public ExplorativePatternMatching(String query, boolean attachData,
    MatchStrategy matchStrategy,
    Traverser traverser,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy) {
    super(query, attachData, LOG);
    this.matchStrategy = matchStrategy;
    this.traverser = traverser;
    this.traverser.setQueryHandler(getQueryHandler());
    this.edgeStepJoinStrategy   = edgeStepJoinStrategy;
    this.vertexStepJoinStrategy = vertexStepJoinStrategy;
  }

  @Override
  protected GraphCollection executeForVertex(LogicalGraph graph) {
    GradoopFlinkConfig config = graph.getConfig();
    GraphHeadFactory graphHeadFactory = config.getGraphHeadFactory();
    VertexFactory vertexFactory = config.getVertexFactory();

    DataSet<Vertex> matchingVertices = graph.getVertices()
      .filter(new MatchingVertices<>(getQuery()));

    if (!doAttachData()) {
      matchingVertices = matchingVertices
        .map(new Id<>())
        .map(new ObjectTo1<>())
        .map(new VertexFromId(vertexFactory));
    }

    DataSet<Tuple2<Vertex, GraphHead>> pairs = matchingVertices
      .map(new AddGraphElementToNewGraph<>(graphHeadFactory))
      .returns(new TupleTypeInfo<>(
        TypeExtractor.getForClass(vertexFactory.getType()),
        TypeExtractor.getForClass(graphHeadFactory.getType())));

    return GraphCollection.fromDataSets(
      pairs.map(new Value1Of2<>()),
      pairs.map(new Value0Of2<>()),
      config);
  }

  @Override
  protected GraphCollection executeForPattern(LogicalGraph graph) {

    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates)
    //--------------------------------------------------------------------------

    DataSet<IdWithCandidates<GradoopId>> vertices = PreProcessor.filterVertices(
      graph, getQuery());

    DataSet<TripleWithCandidates<GradoopId>> edges = PreProcessor.filterEdges(
      graph, getQuery());

    //--------------------------------------------------------------------------
    // Exploration via Traversal
    //--------------------------------------------------------------------------

    TraversalCode traversalCode = traverser.traverse();

    DistributedTraverser<GradoopId> explorer = new DistributedTraverser<>(
      traversalCode,
      traverser.getQueryHandler().getVertexCount(),
      traverser.getQueryHandler().getEdgeCount(),
      getVertexMapping(), getEdgeMapping(),
      edgeStepJoinStrategy, vertexStepJoinStrategy,
      matchStrategy);

    DataSet<Tuple1<Embedding<GradoopId>>> embeddings = explorer
      .traverse(GradoopId.class, vertices, edges);

    //--------------------------------------------------------------------------
    // Post-Processing (build Graph Collection from embeddings)
    //--------------------------------------------------------------------------

    DataSet<Element> elements = embeddings
      .flatMap(new ElementsFromEmbedding(traversalCode,
        graph.getConfig().getGraphHeadFactory(),
        graph.getConfig().getVertexFactory(),
        graph.getConfig().getEdgeFactory()));

    return doAttachData() ?
      PostProcessor.extractGraphCollectionWithData(elements, graph, true) :
      PostProcessor.extractGraphCollection(elements, graph.getConfig(), true);
  }

  @Override
  protected QueryHandler getQueryHandler() {
    return new QueryHandler(getQuery());
  }

  @Override
  public String getName() {
    return ExplorativePatternMatching.class.getName();
  }
}
