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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;

/**
 * Represents a single node in a {@link QueryPlan}
 */
public abstract class PlanNode {
  /**
   * Describes the output of that node.
   */
  private EmbeddingMetaData embeddingMetaData;

  /**
   * Recursively executes this node and returns the resulting {@link Embedding} data set.
   *
   * @return embeddings
   */
  public abstract DataSet<Embedding> execute();

  /**
   * Returns the meta data describing the embeddings produced by this node.
   *
   * @return meta data describing the output
   */
  public EmbeddingMetaData getEmbeddingMetaData() {
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
  protected abstract EmbeddingMetaData computeEmbeddingMetaData();
}
