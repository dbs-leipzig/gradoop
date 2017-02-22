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

package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (graph, pattern->embeddings) => (pattern, 1),..
 */
public class ReportSupportedPatterns
  implements FlatMapFunction<GraphWithPatternEmbeddingsMap, WithCount<int[]>> {

  @Override
  public void flatMap(GraphWithPatternEmbeddingsMap graphEmbeddings,
    Collector<WithCount<int[]>> collector) throws Exception {

    if (! graphEmbeddings.isFrequentPatternCollector()) {
      for (int i = 0; i < graphEmbeddings.getMap().getPatternCount(); i++) {
        int[] pattern = graphEmbeddings.getMap().getKeys()[i];
        collector.collect(new WithCount<>(pattern));
      }
    }
  }
}
