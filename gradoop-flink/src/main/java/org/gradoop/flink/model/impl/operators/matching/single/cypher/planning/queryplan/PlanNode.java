
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

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
