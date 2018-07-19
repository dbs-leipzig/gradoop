/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Input embeddings are extended by an empty path and an additional entry which equals the entry at
 * the expand column. This is done to ensure equally sized embeddings in the case of Expand
 * operations with lower bound 0
 */
public class AdoptEmptyPaths extends RichFlatMapFunction<Embedding, Embedding> {

  /**
   * The column the expansion starts at
   */
  private final int expandColumn;

  /**
   * The column the expanded paths should end at
   */
  private final int closingColumn;

  /**
   * Creates a new UDF instance
   * @param expandColumn column the expantion starts at
   * @param closingColumn column the expanded path should end at
   */
  public AdoptEmptyPaths(int expandColumn, int closingColumn) {
    this.expandColumn = expandColumn;
    this.closingColumn = closingColumn;
  }

  @Override
  public void flatMap(Embedding value, Collector<Embedding> out) throws Exception {
    if (closingColumn >= 0 &&
      !ArrayUtils.isEquals(value.getRawId(expandColumn), value.getRawId(closingColumn))) {
      return;
    }

    value.add();
    value.add(value.getId(expandColumn));
    out.collect(value);
  }
}
