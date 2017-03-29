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

package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions.ElementsFromEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.debug.PrintEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner.greedy.GreedyPlanner;


import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;

import java.util.Map;
import java.util.stream.Collectors;

import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * Implementation of a query engine based on the Cypher graph query language.
 */
public class CypherPatternMatching extends PatternMatching {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(CypherPatternMatching.class);
  /**
   * Morphism strategy for vertex mappings
   */
  private final MatchStrategy vertexStrategy;
  /**
   * Morphism strategy for edge mappings
   */
  private final MatchStrategy edgeStrategy;
  /**
   * Statistics about the data graph
   */
  private final GraphStatistics graphStatistics;

  /**
   * Instantiates a new operator.
   *
   * @param query Cypher query string
   * @param attachData true, if original data shall be attached to the result
   * @param vertexStrategy morphism strategy for vertex mappings
   * @param edgeStrategy morphism strategy for edge mappings
   * @param graphStatistics statistics about the data graph
   */
  public CypherPatternMatching(String query, boolean attachData,
    MatchStrategy vertexStrategy,
    MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    super(query, attachData, LOG);
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.graphStatistics = graphStatistics;
  }

  @Override
  protected GraphCollection executeForVertex(LogicalGraph graph) {
    return executeForPattern(graph);
  }

  @Override
  protected GraphCollection executeForPattern(LogicalGraph graph) {
    // Query planning
    QueryHandler queryHandler = getQueryHandler();
    QueryPlan plan = new GreedyPlanner(graph, queryHandler, graphStatistics,
      vertexStrategy, edgeStrategy).plan().getQueryPlan();

    // Query execution
    DataSet<Embedding> embeddings = plan.execute();
    EmbeddingMetaData embeddingMetaData = plan.getRoot().getEmbeddingMetaData();

    embeddings = log(embeddings, new PrintEmbedding(embeddingMetaData),
      getVertexMapping(), getEdgeMapping());

    // Post processing
    Map<String, Pair<String, String>> sourceTargetVars = queryHandler.getEdges().stream()
      .map(e -> Pair.of(e.getVariable(), Pair.of(
        queryHandler.getVertexById(e.getSourceVertexId()).getVariable(),
        queryHandler.getVertexById(e.getTargetVertexId()).getVariable())))
      .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    DataSet<Element> elements = embeddings
      .flatMap(new ElementsFromEmbedding(
        graph.getConfig().getGraphHeadFactory(),
        graph.getConfig().getVertexFactory(),
        graph.getConfig().getEdgeFactory(),
        embeddingMetaData,
        sourceTargetVars
      ));

    return doAttachData() ?
      PostProcessor.extractGraphCollectionWithData(elements, graph, true) :
      PostProcessor.extractGraphCollection(elements, graph.getConfig(), true);
  }

  @Override
  public String getName() {
    return CypherPatternMatching.class.getName();
  }
}
