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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Triple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Applies a given predicate on a {@link Triple} and projects specified property values to the
 * output embedding.
 */
public class FilterAndProjectTriple extends RichFlatMapFunction<Triple, Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * variable of the source vertex
   */
  private final String sourceVariable;
  /**
   * variable of the target vertex
   */
  private final String targetVariable;
  /**
   * Property keys used for value projection of the source vertex
   */
  private final List<String> sourceProjectionPropertyKeys;
  /**
   * Property keys used for value projection of the edge
   */
  private final List<String> edgeProjectionPropertyKeys;
  /**
   * Property keys used for value projection of the target vertex
   */
  private final List<String> targetProjectionPropertyKeys;
  /**
   * Meta data describing the vertex embedding used for filtering
   */
  private final EmbeddingMetaData filterMetaData;
  /**
   * Source vertex propertyKeys of the embedding used for filtering
   */
  private final List<String> sourceFilterPropertyKeys;
  /**
   * Edge propertyKeys of the embedding used for filtering
   */
  private final List<String> edgeFilterPropertyKeys;
  /**
   * Target vertex propertyKeys of the embedding used for filtering
   */
  private final List<String> targetFilterPropertyKeys;

  /**
   * True if vertex and target variable are the same
   */
  private final boolean isLoop;

  /**
   * Set to true if vertex matching strategy is isomorphism
   */
  private final boolean isVertexIso;

  /**
   * New FilterAndProjectTriples
   * @param sourceVariable the source variable
   * @param edgeVariable edge variabe
   * @param targetVariable target variable
   * @param predicates filter predicates
   * @param projectionPropertyKeys property keys used for projection
   * @param vertexMatchStrategy vertex match strategy
   */
  public FilterAndProjectTriple(String sourceVariable, String edgeVariable, String targetVariable,
    CNF predicates, Map<String, List<String>> projectionPropertyKeys,
    MatchStrategy vertexMatchStrategy) {

    this.predicates = predicates;
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;

    this.sourceProjectionPropertyKeys =
      projectionPropertyKeys.getOrDefault(sourceVariable, new ArrayList<>());
    this.edgeProjectionPropertyKeys =
      projectionPropertyKeys.getOrDefault(edgeVariable, new ArrayList<>());
    this.targetProjectionPropertyKeys =
      projectionPropertyKeys.getOrDefault(targetVariable, new ArrayList<>());

    this.isLoop = sourceVariable.equals(targetVariable);
    this.isVertexIso = vertexMatchStrategy.equals(MatchStrategy.ISOMORPHISM);

    filterMetaData = createFilterMetaData(predicates, sourceVariable, edgeVariable, targetVariable);
    sourceFilterPropertyKeys = filterMetaData.getPropertyKeys(sourceVariable);
    edgeFilterPropertyKeys = filterMetaData.getPropertyKeys(edgeVariable);
    targetFilterPropertyKeys = filterMetaData.getPropertyKeys(targetVariable);

  }

  @Override
  public void flatMap(Triple triple, Collector<Embedding> out) throws Exception {
    boolean isValid = true;

    if (isLoop) {
      if (!(triple.getSourceId().equals(triple.getTargetId()))) {
        isValid = false;
      }
    } else if (isVertexIso && triple.getSourceId().equals(triple.getTargetId())) {
      isValid = false;
    }

    if (isValid && filter(triple)) {
      out.collect(
        EmbeddingFactory.fromTriple(
          triple,
          sourceProjectionPropertyKeys, edgeProjectionPropertyKeys, targetProjectionPropertyKeys,
          sourceVariable, targetVariable
        )
      );
    }
  }

  /**
   * Checks if the the triple holds for the predicate
   * @param triple triple to be filtered
   * @return True if the triple holds for the predicate
   */
  private boolean filter(Triple triple) {
    return predicates.evaluate(
      EmbeddingFactory.fromTriple(triple,
        sourceFilterPropertyKeys,  edgeFilterPropertyKeys, targetFilterPropertyKeys,
        sourceVariable, targetVariable
      ),
      filterMetaData
    );
  }

  /**
   * Creates the {@code EmbeddingMetaData} of the embedding used for filtering
   * @param predicates filter predicates
   * @param sourceVariable source variable
   * @param edgeVariable edge variable
   * @param targetVariable target variable
   * @return filter embedding meta data
   */
  private static EmbeddingMetaData createFilterMetaData(CNF predicates, String sourceVariable,
    String edgeVariable, String targetVariable) {

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn(sourceVariable, EmbeddingMetaData.EntryType.VERTEX, 0);
    metaData.setEntryColumn(edgeVariable, EmbeddingMetaData.EntryType.EDGE, 1);
    metaData.setEntryColumn(targetVariable, EmbeddingMetaData.EntryType.VERTEX, 2);

    int i = 0;
    for (String variable : new String[] {sourceVariable, edgeVariable, targetVariable}) {
      for (String propertyKey : predicates.getPropertyKeys(variable)) {
        metaData.setPropertyColumn(variable, propertyKey, i++);
      }
    }

    return metaData;
  }

}
