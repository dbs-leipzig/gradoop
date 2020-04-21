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
