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

package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

import java.util.Iterator;

/**
 * (graph, pattern -> embedding) => pattern, ...
 */
public class ExpandResult
  implements FlatMapFunction<GraphEmbeddingsPair, WithCount<TraversalCode<String>>> {

  @Override
  public void flatMap(GraphEmbeddingsPair graphEmbeddingsPair,
    Collector<WithCount<TraversalCode<String>>> collector) throws Exception {

    for (TraversalCode<String> code : graphEmbeddingsPair.getPatternEmbeddings().keySet()) {

      int frequencyIndex = code.getTraversals().size() - 1;
      long frequency = code.getTraversals().get(frequencyIndex).getFromTime();

      code.getTraversals().remove(frequencyIndex);

      collector.collect(new WithCount<>(code, frequency));
    }
  }
}
