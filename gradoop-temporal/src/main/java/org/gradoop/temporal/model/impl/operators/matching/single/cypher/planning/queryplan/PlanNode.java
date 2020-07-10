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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

/**
 * Represents a single node in a {@link QueryPlan}
 */
public abstract class PlanNode {
  /**
   * Describes the output of that node.
   */
  private EmbeddingTPGMMetaData embeddingMetaData;

  /**
   * Recursively executes this node and returns the resulting {@link EmbeddingTPGM} data set.
   *
   * @return resulting embeddings
   */
  public abstract DataSet<EmbeddingTPGM> execute();

  /**
   * Returns the meta data describing the embeddings produced by this node.
   *
   * @return meta data describing the output
   */
  public EmbeddingTPGMMetaData getEmbeddingMetaData() {
    if (this.embeddingMetaData == null) {
      this.embeddingMetaData = computeEmbeddingMetaData();
    }
    return embeddingMetaData;
  }

  /**
   * Computes the meta data returned by the specific node.
   *
   * @return meta data
   */
  protected abstract EmbeddingTPGMMetaData computeEmbeddingMetaData();

}
