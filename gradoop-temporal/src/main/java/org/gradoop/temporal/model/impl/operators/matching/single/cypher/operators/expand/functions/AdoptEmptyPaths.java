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

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Input embeddings are extended by an empty path and an additional entry which equals the entry at
 * the expand column. This is done to ensure equally sized embeddings in the case of Expand
 * operations with lower bound 0
 */
public class AdoptEmptyPaths extends RichFlatMapFunction<EmbeddingTPGM, EmbeddingTPGM> {
  /**
   * The column the expansion starts at
   */
  private final int expandColumn;
  /**
   * The column in which the expansion start vertex's time data is stored
   */
  private final int expandVertexTimeColumn;
  /**
   * The column the expanded paths should end at
   */
  private final int closingColumn;

  /**
   * Creates a new instance
   *
   * @param expandColumn  column the expansion starts at
   * @param expandVertexTimeColumn column in which the expansion start vertex's time data is stored
   * @param closingColumn column the expanded path should end at
   */
  public AdoptEmptyPaths(int expandColumn, int expandVertexTimeColumn, int closingColumn) {
    this.expandColumn = expandColumn;
    this.expandVertexTimeColumn = expandVertexTimeColumn;
    this.closingColumn = closingColumn;
  }

  @Override
  public void flatMap(EmbeddingTPGM value, Collector<EmbeddingTPGM> out) {
    if (closingColumn >= 0 &&
      !ArrayUtils.isEquals(value.getRawId(expandColumn), value.getRawId(closingColumn))) {
      return;
    }

    value.add();
    value.add(value.getId(expandColumn));
    Long[] timeData = value.getTimes(expandVertexTimeColumn);
    //TODO add empty time data?
    value.addTimeData(timeData[0], timeData[1], timeData[2], timeData[3]);
    out.collect(value);
  }
}
