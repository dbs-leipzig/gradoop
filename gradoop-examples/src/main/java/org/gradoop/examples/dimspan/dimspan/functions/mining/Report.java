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

package org.gradoop.examples.dimspan.dimspan.functions.mining;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.gradoop.examples.dimspan.dimspan.config.DIMSpanConfig;
import org.gradoop.examples.dimspan.dimspan.config.DataflowStep;
import org.gradoop.examples.dimspan.dimspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (graph, pattern->embeddings) => (pattern, 1),..
 */
public class Report implements FlatMapFunction<GraphEmbeddingsPair, WithCount<int[]>> {

  private final boolean compress;

  public Report(DIMSpanConfig fsmConfig) {
    compress = fsmConfig.getPatternCompressionInStep() == DataflowStep.MAP;
  }

  @Override
  public void flatMap(GraphEmbeddingsPair graphEmbeddings,
    Collector<WithCount<int[]>> collector) throws Exception {

    if (! graphEmbeddings.isCollector()) {
      for (int i = 0; i < graphEmbeddings.getPatternEmbeddings().getPatternCount(); i++) {
        int[] pattern = graphEmbeddings.getPatternEmbeddings().getPatternData()[i];
        collector.collect(new WithCount<>(pattern));
      }
    }
  }
}
