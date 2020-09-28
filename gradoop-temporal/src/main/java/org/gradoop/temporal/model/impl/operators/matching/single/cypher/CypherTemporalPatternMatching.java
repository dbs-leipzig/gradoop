/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.debug.PrintEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add.AddEmbeddingsElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.ProjectEmbeddingsElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions.ElementsFromEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.planner.greedy.GreedyPlanner;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * Implementation of a query engine based on the Cypher graph query language.
 */
public class CypherTemporalPatternMatching
  extends TemporalPatternMatching<TemporalGraphHead, TemporalGraph, TemporalGraphCollection> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(CypherTemporalPatternMatching.class);
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
  private final TemporalGraphStatistics graphStatistics;

  /**
   * Instantiates a new operator.
   *
   * @param query           Cypher query string
   * @param attachData      true, if original data shall be attached to the result
   * @param vertexStrategy  morphism strategy for vertex mappings
   * @param edgeStrategy    morphism strategy for edge mappings
   * @param graphStatistics statistics about the data graph
   * @param postprocessor   postprocessing pipeline for the query CNF
   */
  public CypherTemporalPatternMatching(String query, boolean attachData,
                                       MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
                                       TemporalGraphStatistics graphStatistics,
                                       CNFPostProcessing postprocessor) {
    this(query, null, attachData, vertexStrategy, edgeStrategy, graphStatistics, postprocessor);
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
   * @param postprocessor       postprocessing pipeline for the query CNF
   */
  public CypherTemporalPatternMatching(String query, String constructionPattern, boolean attachData,
                                       MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
                                       TemporalGraphStatistics graphStatistics,
                                       CNFPostProcessing postprocessor) {
    super(query, attachData, postprocessor, LOG);
    this.constructionPattern = constructionPattern;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.graphStatistics = graphStatistics;
  }

  @Override
  protected TemporalGraphCollection executeForVertex(TemporalGraph graph) {
    return executeForPattern(graph);
  }

  @Override
  protected TemporalGraphCollection executeForPattern(TemporalGraph graph) {
    // Query planning
    TemporalQueryHandler queryHandler = getQueryHandler();
    QueryPlan plan =
      new GreedyPlanner(graph, queryHandler, graphStatistics, vertexStrategy, edgeStrategy).plan()
        .getQueryPlan();

    // Query execution
    DataSet<Embedding> embeddings = plan.execute();

    EmbeddingMetaData embeddingMetaData = plan.getRoot().getEmbeddingMetaData();

    embeddings =
      log(embeddings, new PrintEmbedding(embeddingMetaData),
        getVertexMapping(), getEdgeMapping());

    // Pattern construction (if necessary)
    DataSet<Element> finalElements = this.constructionPattern != null ?
      constructFinalElements(graph, embeddings, embeddingMetaData) :
      embeddings.flatMap(
        new ElementsFromEmbeddingTPGM<TemporalGraphHead, TemporalVertex, TemporalEdge>(
          graph.getFactory().getGraphHeadFactory(),
          graph.getFactory().getVertexFactory(),
          graph.getFactory().getEdgeFactory(),
          embeddingMetaData,
          queryHandler.getSourceTargetVariables()));

    // Post processing
    TemporalGraphCollection graphCollection = doAttachData() ?
      PostProcessor.<TemporalGraphHead,
        TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection>
        extractGraphCollectionWithData(
          finalElements, graph, true) :
      PostProcessor.<TemporalGraphHead,
        TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection>
        extractGraphCollection(finalElements, graph.getCollectionFactory(),
          true);

    return graphCollection;
  }

  @Override
  protected TemporalGraphCollection emptyCollection(TemporalGraph graph) {
    return graph.getCollectionFactory().createEmptyCollection();
  }

  /**
   * Method to construct final embedded elements
   *
   * @param graph             Used logical graph
   * @param embeddings        embeddings
   * @param embeddingMetaData Meta information
   * @return New set of EmbeddingElements
   */
  private DataSet<Element> constructFinalElements(TemporalGraph graph, DataSet<Embedding> embeddings,
                                                  EmbeddingMetaData embeddingMetaData) {

    TemporalQueryHandler constructionPatternHandler = null;
    try {
      // no postprocessing needed, construction pattern is not a query
      constructionPatternHandler = new TemporalQueryHandler(
        this.constructionPattern, new CNFPostProcessing(new ArrayList<>()));
      // will never happen, as the construction pattern does not contain conditions
    } catch (QueryContradictoryException e) {
      e.printStackTrace();
    }
    Objects.requireNonNull(constructionPatternHandler).updateGeneratedVariableNames(n -> "_" + n);

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
      new ElementsFromEmbeddingTPGM(
        graph.getFactory().getGraphHeadFactory(),
        graph.getFactory().getVertexFactory(),
        graph.getFactory().getEdgeFactory(),
        newMetaData,
        constructionPatternHandler.getSourceTargetVariables(),
        constructionPatternHandler.getLabelsForVariables(newVars)));
  }

  /**
   * Compute new meta information
   *
   * @param metaData             old meta information
   * @param returnPatternHandler pattern handler
   * @param existingVariables    old variables
   * @param newVariables         new variables
   * @return new EmbeddingMetaData
   */
  private EmbeddingMetaData computeNewMetaData(EmbeddingMetaData metaData,
                                                   TemporalQueryHandler returnPatternHandler,
                                                   Set<String> existingVariables, Set<String> newVariables) {
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


}
