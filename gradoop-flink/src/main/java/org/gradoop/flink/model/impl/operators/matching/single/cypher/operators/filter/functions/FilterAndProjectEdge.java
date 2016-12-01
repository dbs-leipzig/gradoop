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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.Filter;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.Projector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Filters a set of edges and projects the remaining elements.
 * Edge -> Embedding(IdEntry(SrcId), ProjectionEntry(Edge), IdEntry(TargetId))
 */
public class FilterAndProjectEdge extends RichFlatMapFunction<Edge, Embedding> {
  /**
   * Predicate used for filtering in CNF
   */
  private final CNF predicates;
  /**
   * Holds a list of property keys for every embedding entry
   * The specified properties will be kept in the projection
   */
  private final Map<Integer, List<String>> propertyKeyMapping = new HashMap<>();
  /**
   * Maps the predicates variables to embedding entries
   */
  private final Map<String, Integer> columnMapping = new HashMap<>();

  /**
   * Create a new edge filter and project function
   * @param predicates filter predicates
   * @param propertyKeys projection properties
   */
  public FilterAndProjectEdge(CNF predicates, List<String> propertyKeys) {
    this.predicates = predicates;
    propertyKeyMapping.put(1, propertyKeys);

    String variable = Lists.newArrayList(predicates.getVariables()).get(0);
    columnMapping.put(variable, 1);
  }

  @Override
  public void flatMap(Edge edge, Collector<Embedding> out) throws Exception {
    Embedding embedding = Embedding.fromEdge(edge);
    if (Filter.filter(predicates, embedding, columnMapping)) {
      Projector.project(embedding, propertyKeyMapping);

      out.collect(embedding);
    }
  }
}
