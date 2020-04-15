package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;


import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterTemporalEmbedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;


/**
 * Filters a set of TPGM Embeddings by the given predicates
 * The resulting embeddings have the same schema as the input embeddings
 */
public class FilterTemporalEmbeddings implements PhysicalTPGMOperator {
    /**
     * Candidate Embeddings
     */
    private final DataSet<EmbeddingTPGM> input;
    /**
     * Predicates in conjunctive normal form
     */
    private CNF predicates;
    /**
     * Maps variable names to embedding entries;
     */
    private final EmbeddingTPGMMetaData metaData;

    /**
     * Operator name used for Flink operator description
     */
    private String name;

    /**
     * New embedding filter operator
     * @param input Candidate embeddings
     * @param predicates Predicates to used for filtering
     * @param metaData Maps variable names to embedding entries
     */
    public FilterTemporalEmbeddings(DataSet<EmbeddingTPGM> input, CNF predicates,
                                    EmbeddingTPGMMetaData metaData) {
        this.input = input;
        this.predicates = predicates;
        this.metaData = metaData;
        this.setName("FilterEmbeddings");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return input
                .filter(new FilterTemporalEmbedding(predicates, metaData))
                .name(getName());
    }

    @Override
    public void setName(String newName) {
        this.name = newName;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
