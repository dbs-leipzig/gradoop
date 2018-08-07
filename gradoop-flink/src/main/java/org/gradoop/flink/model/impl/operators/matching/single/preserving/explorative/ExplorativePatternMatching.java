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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.log4j.Logger;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromId;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.functions.AddGraphElementToNewGraph;
import org.gradoop.flink.model.impl.operators.matching.common.functions.ElementsFromEmbedding;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.SetPairBulkTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.SetPairForLoopTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.SetPairTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TripleForLoopTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TripleTraverser;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Objects;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES;

/**
 * Algorithm detects subgraphs by traversing the search graph according to a
 * given traversal code which is derived from the query pattern.
 */
public class ExplorativePatternMatching
  extends PatternMatching
  implements UnaryGraphToCollectionOperator {
  /**
   * Name for broadcast set which contains the superstep id.
   */
  public static final String BC_SUPERSTEP = "bc_superstep";
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(ExplorativePatternMatching.class);
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
   * Strategy for vertex and edge mappings
   */
  private final MatchStrategy matchStrategy;
  /**
   * Strategy iterating the graph
   */
  private final TraverserStrategy traverserStrategy;

  /**
   * Create new operator instance
   *
   * @param query                   GDL query graph
   * @param attachData              true, if original data shall be attached
   *                                to the result
   * @param matchStrategy           match strategy for vertex and edge mappings
   * @param traverserStrategy       iteration strategy for distributed traversal
   * @param traverser               Traverser used for the query graph
   * @param edgeStepJoinStrategy    Join strategy for edge extension
   * @param vertexStepJoinStrategy  Join strategy for vertex extension
   */
  private ExplorativePatternMatching(String query, boolean attachData,
    MatchStrategy matchStrategy,
    TraverserStrategy traverserStrategy,
    Traverser traverser,
    JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy) {
    super(query, attachData, LOG);
    this.matchStrategy          = matchStrategy;
    this.traverserStrategy = traverserStrategy;
    this.traverser = traverser;
    this.traverser.setQueryHandler(getQueryHandler());
    this.edgeStepJoinStrategy   = edgeStepJoinStrategy;
    this.vertexStepJoinStrategy = vertexStepJoinStrategy;
  }

  @Override
  protected GraphCollection executeForVertex(LogicalGraph graph) {
    GradoopFlinkConfig config = graph.getConfig();
    EPGMGraphHeadFactory<GraphHead> graphHeadFactory = config.getGraphHeadFactory();
    EPGMVertexFactory<Vertex> vertexFactory = config.getVertexFactory();
    String variable = getQueryHandler().getVertices().iterator().next().getVariable();

    DataSet<Vertex> matchingVertices = graph.getVertices()
      .filter(new MatchingVertices<>(getQuery()));

    if (!doAttachData()) {
      matchingVertices = matchingVertices
        .map(new Id<>())
        .map(new ObjectTo1<>())
        .map(new VertexFromId(vertexFactory));
    }

    DataSet<Tuple2<Vertex, GraphHead>> pairs = matchingVertices
      .map(new AddGraphElementToNewGraph<>(graphHeadFactory, variable))
      .returns(new TupleTypeInfo<>(
        TypeExtractor.getForClass(vertexFactory.getType()),
        TypeExtractor.getForClass(graphHeadFactory.getType())));

    return config.getGraphCollectionFactory().fromDataSets(
      pairs.map(new Value1Of2<>()),
      pairs.map(new Value0Of2<>()));
  }

  @Override
  protected GraphCollection executeForPattern(LogicalGraph graph) {

    TraversalCode traversalCode = traverser.traverse();

    DataSet<Tuple1<Embedding<GradoopId>>> embeddings;

    if (traverserStrategy == TraverserStrategy.SET_PAIR_BULK_ITERATION ||
      traverserStrategy == TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION) {

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

      SetPairTraverser<GradoopId> distributedTraverser;

      if (traverserStrategy == TraverserStrategy.SET_PAIR_BULK_ITERATION) {
        distributedTraverser = new SetPairBulkTraverser<>(traversalCode, matchStrategy,
          traverser.getQueryHandler().getVertexCount(), traverser.getQueryHandler().getEdgeCount(),
          GradoopId.class, edgeStepJoinStrategy, vertexStepJoinStrategy, getVertexMapping(),
          getEdgeMapping());
      } else {
        distributedTraverser = new SetPairForLoopTraverser<>(traversalCode, matchStrategy,
          traverser.getQueryHandler().getVertexCount(), traverser.getQueryHandler().getEdgeCount(),
          GradoopId.class, edgeStepJoinStrategy, vertexStepJoinStrategy, getVertexMapping(),
          getEdgeMapping());
      }

      embeddings = distributedTraverser.traverse(vertices, edges);
    } else if (traverserStrategy == TraverserStrategy.TRIPLES_FOR_LOOP_ITERATION) {
      DataSet<TripleWithCandidates<GradoopId>> triples = PreProcessor
        .filterTriplets(graph, getQuery());

      TripleTraverser<GradoopId> distributedTraverser = new TripleForLoopTraverser<>(
        traversalCode, matchStrategy,
        traverser.getQueryHandler().getVertexCount(),
        traverser.getQueryHandler().getEdgeCount(),
        GradoopId.class, edgeStepJoinStrategy, getVertexMapping(), getEdgeMapping());

      embeddings = distributedTraverser.traverse(triples);

    } else {
      throw new IllegalArgumentException("Unsupported traverser strategy: " + traverserStrategy);
    }

    //--------------------------------------------------------------------------
    // Post-Processing (build Graph Collection from embeddings)
    //--------------------------------------------------------------------------

    DataSet<Element> elements = embeddings
      .flatMap(new ElementsFromEmbedding(traversalCode,
        graph.getConfig().getGraphHeadFactory(),
        graph.getConfig().getVertexFactory(),
        graph.getConfig().getEdgeFactory(),
        getQueryHandler()
      ));

    return doAttachData() ?
      PostProcessor.extractGraphCollectionWithData(elements, graph, true) :
      PostProcessor.extractGraphCollection(elements, graph.getConfig(), true);
  }

  @Override
  public String getName() {
    return ExplorativePatternMatching.class.getName();
  }


  /**
   * Used for configuring and creating a new {@link ExplorativePatternMatching}
   * operator instance.
   */
  public static final class Builder {
    /**
     * GDL query string
     */
    private String query;
    /**
     * Attach original vertex and edge data
     */
    private boolean attachData;
    /**
     * Matching strategy for vertex and edge mappings
     */
    private MatchStrategy matchStrategy;
    /**
     * Iteration strategy for traversing the graph
     */
    private TraverserStrategy traverserStrategy;
    /**
     * Provides a traversal description for the distributed traverser
     */
    private Traverser traverser;
    /**
     * Join strategy for edge extensions during traversal
     */
    private JoinOperatorBase.JoinHint edgeStepJoinStrategy;
    /**
     * Join strategy for vertex extensions during traversal
     */
    private JoinOperatorBase.JoinHint vertexStepJoinStrategy;

    /**
     * Creates a new builder instance
     */
    public Builder() {
      this.attachData             = false;
      this.matchStrategy          = MatchStrategy.ISOMORPHISM;
      this.traverserStrategy = TraverserStrategy.SET_PAIR_BULK_ITERATION;
      this.traverser              = new DFSTraverser();
      this.edgeStepJoinStrategy   = OPTIMIZER_CHOOSES;
      this.vertexStepJoinStrategy = OPTIMIZER_CHOOSES;
    }

    /**
     * Set the GDL query string. e.g. "(a)-->(b)"
     *
     * @param query GDL query
     * @return modified builder
     */
    public Builder setQuery(String query) {
      this.query = query;
      return this;
    }

    /**
     * Set if the original vertex and edge data shall be attached to the result.
     *
     * @param attachData true, iff data shall be attached
     * @return modified builder
     */
    public Builder setAttachData(boolean attachData) {
      this.attachData = attachData;
      return this;
    }

    /**
     * Set matching strategy for vertex and edge embeddings (e.g. isomorphism).
     *
     * @param matchStrategy matching strategy
     * @return modified builder
     */
    public Builder setMatchStrategy(MatchStrategy matchStrategy) {
      this.matchStrategy = matchStrategy;
      return this;
    }

    /**
     * Set iteration strategy for traversing the graph (e.g. bulk traversal).
     *
     * @param traverserStrategy iteration strategy
     * @return modified builder
     */
    public Builder setTraverserStrategy(TraverserStrategy traverserStrategy) {
      this.traverserStrategy = traverserStrategy;
      return this;
    }

    /**
     * Sets the traverser to describe the distributed graph traversal.
     *
     * @param traverser traverser for the query graph
     * @return modified builder
     */
    public Builder setTraverser(Traverser traverser) {
      this.traverser = traverser;
      return this;
    }

    /**
     * Sets the join strategy for joining edges during traversal.
     *
     * @param edgeStepJoinStrategy join strategy
     * @return modified builder
     */
    public Builder setEdgeStepJoinStrategy(
      JoinOperatorBase.JoinHint edgeStepJoinStrategy) {
      this.edgeStepJoinStrategy = edgeStepJoinStrategy;
      return this;
    }

    /**
     * Sets the join strategy for joining vertices during traversal.
     *
     * @param vertexStepJoinStrategy join strategy
     * @return modified builder
     */
    public Builder setVertexStepJoinStrategy(
      JoinOperatorBase.JoinHint vertexStepJoinStrategy) {
      this.vertexStepJoinStrategy = vertexStepJoinStrategy;
      return this;
    }

    /**
     * Instantiates a new {@link ExplorativePatternMatching} operator.
     *
     * @return operator instance
     */
    public ExplorativePatternMatching build() {
      Objects.requireNonNull(query, "Missing GDL query");
      Objects.requireNonNull(matchStrategy, "Missing match strategy");
      Objects.requireNonNull(traverserStrategy, "Missing iteration strategy");
      Objects.requireNonNull(traverser, "Missing traverser");
      Objects.requireNonNull(edgeStepJoinStrategy, "Missing join strategy");
      Objects.requireNonNull(vertexStepJoinStrategy, "Missing join strategy");

      return new ExplorativePatternMatching(query, attachData,
        matchStrategy, traverserStrategy, traverser, edgeStepJoinStrategy,
        vertexStepJoinStrategy);
    }

  }
}
