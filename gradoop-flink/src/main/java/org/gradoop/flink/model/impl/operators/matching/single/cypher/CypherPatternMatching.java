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
package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.debug.PrintEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions.ElementsFromEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add.AddEmbeddingsElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.ProjectEmbeddingsElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner.greedy.GreedyPlanner;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.Set;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
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
   * Construction pattern for result transformation.
   */
  private final String constructionPattern;
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
   * @param query           Cypher query string
   * @param attachData      true, if original data shall be attached to the result
   * @param vertexStrategy  morphism strategy for vertex mappings
   * @param edgeStrategy    morphism strategy for edge mappings
   * @param graphStatistics statistics about the data graph
   */
  public CypherPatternMatching(String query, boolean attachData, MatchStrategy vertexStrategy,
    MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    this(query, null, attachData, vertexStrategy, edgeStrategy, graphStatistics);
  }

  /**
   * Instantiates a new operator.
   *
   * @param query               Cypher query string
   * @param constructionPattern Construction pattern
   * @param attachData          true, if original data shall be attached to the result
   * @param vertexStrategy      morphism strategy for vertex mappings
   * @param edgeStrategy        morphism strategy for edge mappings
   * @param graphStatistics     statistics about the data graph
   */
  public CypherPatternMatching(String query, String constructionPattern, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    super(query, attachData, LOG);
    this.constructionPattern = constructionPattern;
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
    QueryPlan plan =
      new GreedyPlanner(graph, queryHandler, graphStatistics, vertexStrategy, edgeStrategy).plan()
        .getQueryPlan();

    // Query execution
    DataSet<Embedding> embeddings = plan.execute();
    EmbeddingMetaData embeddingMetaData = plan.getRoot().getEmbeddingMetaData();

    embeddings =
      log(embeddings, new PrintEmbedding(embeddingMetaData), getVertexMapping(), getEdgeMapping());

    // Pattern construction (if necessary)
    DataSet<Element> finalElements = this.constructionPattern != null ?
      constructFinalElements(graph, embeddings, embeddingMetaData) :
      embeddings.flatMap(
        new ElementsFromEmbedding(
          graph.getConfig().getGraphHeadFactory(),
          graph.getConfig().getVertexFactory(),
          graph.getConfig().getEdgeFactory(),
          embeddingMetaData,
          queryHandler.getSourceTargetVariables()));

    // Post processing
    return
      doAttachData() ? PostProcessor.extractGraphCollectionWithData(finalElements, graph, true) :
      PostProcessor.extractGraphCollection(finalElements, graph.getConfig(), true);
  }

  /**
   * Method to construct final embedded elements
   *
   * @param graph               Used logical graph
   * @param embeddings          embeddings
   * @param embeddingMetaData   Meta information
   * @return                    New set of EmbeddingElements
   */
  private DataSet<Element> constructFinalElements(LogicalGraph graph, DataSet<Embedding> embeddings,
    EmbeddingMetaData embeddingMetaData) {

    QueryHandler constructionPatternHandler = new QueryHandler(this.constructionPattern);

    Set<String> queryVars = Sets.newHashSet(embeddingMetaData.getVariables());
    Set<String> constructionVars = constructionPatternHandler.getAllVariables();
    Set<String> existingVars = intersection(queryVars, constructionVars).immutableCopy();
    Set<String> newVars = difference(constructionVars, queryVars).immutableCopy();

    EmbeddingMetaData newMetaData = computeNewMetaData(
      embeddingMetaData, constructionPatternHandler, existingVars, newVars);

    // project existing embedding elements to new embeddings
    ProjectEmbeddingsElements projectedEmbeddings =
      new ProjectEmbeddingsElements(embeddings, existingVars, embeddingMetaData, newMetaData);
    // add new embedding elements
    AddEmbeddingsElements addEmbeddingsElements =
      new AddEmbeddingsElements(projectedEmbeddings.evaluate(), newVars.size());

    return addEmbeddingsElements.evaluate().flatMap(
      new ElementsFromEmbedding(
        graph.getConfig().getGraphHeadFactory(),
        graph.getConfig().getVertexFactory(),
        graph.getConfig().getEdgeFactory(),
        newMetaData,
        constructionPatternHandler.getSourceTargetVariables(),
        constructionPatternHandler.getLabelsForVariables(newVars)));
  }

  /**
   * Compute new meta information
   *
   * @param metaData              old meta information
   * @param returnPatternHandler  pattern handler
   * @param existingVariables     old variables
   * @param newVariables          new variables
   * @return                      new EmbeddingMetaData
   */
  private EmbeddingMetaData computeNewMetaData(EmbeddingMetaData metaData,
    QueryHandler returnPatternHandler, Set<String> existingVariables, Set<String> newVariables) {
    // update meta data
    EmbeddingMetaData newMetaData = new EmbeddingMetaData();

    // case 1: Filter existing embeddings based on return pattern
    for (String var : existingVariables) {
      newMetaData.setEntryColumn(var, metaData.getEntryType(var), newMetaData.getEntryCount());
    }

    // case 2: Add new vertices and edges
    for (String var : newVariables) {
      EmbeddingMetaData.EntryType type = returnPatternHandler.isEdge(var) ?
        EmbeddingMetaData.EntryType.EDGE :
        EmbeddingMetaData.EntryType.VERTEX;

      newMetaData.setEntryColumn(var, type, newMetaData.getEntryCount());
    }
    return newMetaData;
  }

  @Override
  public String getName() {
    return CypherPatternMatching.class.getName();
  }
}
