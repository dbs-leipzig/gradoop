/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Post-processes the expand iteration results
 *
 * <ol>
 * <li>Remove paths below lower bound length</li>
 * <li>Remove results that do not match circle condition</li>
 * <li>Turn intermediate results into embeddings</li>
 * </ol>
 */
public class PostProcessExpandEmbeddingTPGM extends RichFlatMapFunction<
  ExpandEmbeddingTPGM, EmbeddingTPGM> {

  /**
   * Holds the minimum path size calculated from lower bound
   */
  private final int minPathLength;
  /**
   * Specifies the base column which should be equal to the paths end column
   */
  private final int closingColumn;

  /**
   * Create a new Post-process function
   *
   * @param lowerBound    the lower bound path length
   * @param closingColumn the base column which should equal the paths end column
   */
  public PostProcessExpandEmbeddingTPGM(int lowerBound, int closingColumn) {
    this.minPathLength = lowerBound * 2 - 1;
    this.closingColumn = closingColumn;
  }

  @Override
  public void flatMap(ExpandEmbeddingTPGM value, Collector<EmbeddingTPGM> out) throws Exception {
    if (value.pathSize() < minPathLength) {
      return;
    }

    if (closingColumn >= 0 && !value.getBase().getId(closingColumn).equals(value.getEndVertex())) {
      return;
    }

    out.collect(value.toEmbeddingTPGM());
  }
}
