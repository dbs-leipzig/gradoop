package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.FilterTemporalEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

/**
 * Unary nodes that wraps a {@link FilterTemporalEmbeddings} operator.
 */
public class FilterTemporalEmbeddingsNode extends UnaryNode implements FilterNode {
    /**
     * Filter predicate that is applied on the embedding
     */
    private CNF filterPredicate;
    /**
     * Creates a new node.
     *
     * @param childNode input plan node
     * @param filterPredicate filter predicate to be applied on embeddings
     */
    public FilterTemporalEmbeddingsNode(PlanNode childNode, CNF filterPredicate) {
        super(childNode);
        this.filterPredicate = filterPredicate;
    }

    @Override
    public DataSet<EmbeddingTPGM> execute() {
        FilterTemporalEmbeddings op =
                new FilterTemporalEmbeddings(getChildNode().execute(), filterPredicate, getEmbeddingMetaData());
        op.setName(toString());
        return op.evaluate();
    }

    /**
     * Returns a copy of the filter predicate attached to this node.
     *
     * @return filter predicate
     */
    public CNF getFilterPredicate() {
        return new CNF(filterPredicate);
    }

    @Override
    protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
        return new EmbeddingTPGMMetaData(getChildNode().getEmbeddingMetaData());
    }

    @Override
    public String toString() {
        return String.format("FilterTemporalEmbeddingsNode{filterPredicate=%s}", filterPredicate);
    }
}
