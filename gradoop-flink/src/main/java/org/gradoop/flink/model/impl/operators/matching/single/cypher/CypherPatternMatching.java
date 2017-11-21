/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.functions
  .ElementsFromEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.add
  .AddEmbeddingsElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter
  .FilterEmbeddingsElements;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner.greedy
  .GreedyPlanner;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.Edge;

import java.util.HashMap;
import java.util.List;
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

  private final String returnPattern;
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
    super(query, attachData, LOG);
    this.returnPattern = null;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.graphStatistics = graphStatistics;
  }

  public CypherPatternMatching(String query, String returnPattern, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    super(query, attachData, LOG);
    this.returnPattern = returnPattern;
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

    // TODO: apply return pattern to embeddings
    final QueryHandler returnPatternHandler;
    final List<String> newElementVariables;
    if (this.returnPattern != null) {
      returnPatternHandler = new QueryHandler(this.returnPattern);
      Tuple3<DataSet<Embedding>, EmbeddingMetaData, List<String>> returnPatternResult =
        applyReturnPattern(embeddings, embeddingMetaData, returnPatternHandler);
      embeddings = returnPatternResult.f0;
      embeddingMetaData = returnPatternResult.f1;
      newElementVariables = returnPatternResult.f2;
    } else {
      returnPatternHandler = queryHandler;
      newElementVariables = null;
    }

    // Post processing
    Map<String, Pair<String, String>> sourceTargetVars = returnPatternHandler.getEdges().stream()
      .map(e -> Pair.of(e.getVariable(), Pair
        .of(returnPatternHandler.getVertexById(e.getSourceVertexId()).getVariable(),
          returnPatternHandler.getVertexById(e.getTargetVertexId()).getVariable())))
      .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    // Map labels to EPGM elements. This is needed for newly created return pattern elements
    Map<String, String> elementsLabels;
    if (newElementVariables != null) {
      elementsLabels = newElementVariables.stream().map(var -> {
        if (returnPatternHandler.getEdgeByVariable(var) != null) {
          return Pair.of(var, returnPatternHandler.getEdgeByVariable(var).getLabel());
        } else {
          return Pair.of(var, returnPatternHandler.getVertexByVariable(var).getLabel());
        }
      }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    } else {
      elementsLabels = new HashMap<>();
    }

    DataSet<Element> elements = embeddings.flatMap(
      new ElementsFromEmbedding(
        graph.getConfig().getGraphHeadFactory(),
        graph.getConfig().getVertexFactory(),
        graph.getConfig().getEdgeFactory(),
        embeddingMetaData,
        sourceTargetVars,
        elementsLabels));

    return doAttachData() ? PostProcessor.extractGraphCollectionWithData(elements, graph, true) :
      PostProcessor.extractGraphCollection(elements, graph.getConfig(), true);
  }

  @Override
  public String getName() {
    return CypherPatternMatching.class.getName();
  }

  public Tuple3<DataSet<Embedding>, EmbeddingMetaData, List<String>> applyReturnPattern(
    DataSet<Embedding> embeddings, EmbeddingMetaData metaData, QueryHandler returnPatternHandler) {
    List<String> queryVariables = metaData.getVariables();
    List<String> returnPatternVariables =
      returnPatternHandler.getVertices().stream().map(v -> v.getVariable())
        .collect(Collectors.toList());
    returnPatternVariables.addAll(returnPatternHandler.getEdges().stream().map(e -> e.getVariable())
      .collect(Collectors.toList()));

    List<String> existingVariables =
      returnPatternVariables.stream().filter(var -> queryVariables.contains(var))
        .collect(Collectors.toList());
    List<String> newVariables =
      returnPatternVariables.stream().filter(var -> !queryVariables.contains(var))
        .collect(Collectors.toList());

    DataSet<Embedding> newEmbeddings;
    EmbeddingMetaData newMetaData = new EmbeddingMetaData();


    // case 1: Filter existing embeddings based on return pattern
    for (String var : existingVariables) {
      newMetaData.setEntryColumn(var, metaData.getEntryType(var), newMetaData.getEntryCount());
    }

    FilterEmbeddingsElements filterEmbeddingsElements =
      new FilterEmbeddingsElements(embeddings, existingVariables, metaData, newMetaData);
    newEmbeddings = filterEmbeddingsElements.evaluate();


    // case 2: Add new vertices and edges
    for (String var : newVariables) {
      Edge edge = returnPatternHandler.getEdgeByVariable(var);
      EmbeddingMetaData.EntryType type;

      if (edge != null) {
        type = EmbeddingMetaData.EntryType.EDGE;
      } else {
        type = EmbeddingMetaData.EntryType.VERTEX;
      }
      newMetaData.setEntryColumn(var, type, newMetaData.getEntryCount());
    }

    AddEmbeddingsElements addEmbeddingsElements =
      new AddEmbeddingsElements(newEmbeddings, newVariables);
    newEmbeddings = addEmbeddingsElements.evaluate();

    return new Tuple3<>(newEmbeddings, newMetaData, newVariables);
  }
}
