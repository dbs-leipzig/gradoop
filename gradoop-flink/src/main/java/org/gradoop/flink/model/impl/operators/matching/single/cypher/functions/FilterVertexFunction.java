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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.Filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Filters a set of vertices by given predicates
 */
public class FilterVertexFunction extends RichFlatMapFunction<Vertex, Embedding> {
  /**
   * Predicates used for filtering
   */
  private final CNF predicates;
  /**
   * Mapping of variables names to embedding column
   * The predicate should only hold one variables which we map to column 0
   */
  private final Map<String, Integer> columnMapping = new HashMap<>();

  /**
   * New vertex filter function
   * @param predicates predicates used for filtering
   */
  public FilterVertexFunction(CNF predicates) {
    this.predicates = predicates;

    String variable = Lists.newArrayList(predicates.getVariables()).get(0);
    columnMapping.put(variable, 0);
  }

  @Override
  public void flatMap(Vertex value, Collector<Embedding> out) throws Exception {
    if (Filter.filter(predicates, Embedding.fromVertex(value), columnMapping)) {

      Embedding embedding = new Embedding();
      embedding.addEntry(new IdEntry(value.getId()));
      out.collect(embedding);

    }
  }
}
