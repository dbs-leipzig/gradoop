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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.Filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Filters a set of edges by given predicates
 */
public class FilterEdge extends RichFlatMapFunction<Edge, Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Mapping of variables names to embedding column
   * The predicate should only hold one variables which we map to column 1
   */
  private final Map<String, Integer> columnMapping = new HashMap<>();

  /**
   * New edge filter function
   * @param predicates predicates used for filtering
   */
  public FilterEdge(CNF predicates) {
    this.predicates = predicates;

    String variable = Lists.newArrayList(predicates.getVariables()).get(0);
    columnMapping.put(variable, 1);
  }

  @Override
  public void flatMap(Edge edge, Collector<Embedding> out) throws Exception {
    if (Filter.filter(predicates, Embedding.fromEdge(edge), columnMapping)) {

      Embedding embedding = new Embedding();
      embedding.addEntry(new IdEntry(edge.getSourceId()));
      embedding.addEntry(new IdEntry(edge.getId()));
      embedding.addEntry(new IdEntry(edge.getTargetId()));

      out.collect(embedding);

    }
  }
}
