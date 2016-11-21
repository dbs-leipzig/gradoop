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

package org.gradoop.flink.algorithms.fsm2.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;

import java.util.Set;

/**
 * transaction -> (edgeLabel,1L),..
 */
public class EdgeLabels
  implements FlatMapFunction<GraphTransaction, WithCount<String>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private WithCount<String> reuseTuple = new WithCount<>(null, 1);

  @Override
  public void flatMap(GraphTransaction graph,
    Collector<WithCount<String>> out) throws Exception {

    Set<String> edgeLabels = Sets.newHashSet();

    for (Edge edge : graph.getEdges()) {
      edgeLabels.add(edge.getLabel());
    }

    for (String label : edgeLabels) {
      reuseTuple.setObject(label);
      out.collect(reuseTuple);
    }
  }
}
