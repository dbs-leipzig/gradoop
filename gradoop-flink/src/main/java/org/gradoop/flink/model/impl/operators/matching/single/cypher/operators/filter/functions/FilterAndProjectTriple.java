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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Triple;

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
