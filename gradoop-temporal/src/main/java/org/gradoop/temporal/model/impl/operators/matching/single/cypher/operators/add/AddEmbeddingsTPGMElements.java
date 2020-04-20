package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add.functions.AddEmbeddingTPGMElements;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import java.util.List;

/**
 * Adds {@link EmbeddingTPGM} entries to a given {@link EmbeddingTPGM} based on a {@link List}
 * of variables via an {@link EmbeddingTPGMMetaData} object.
 */
public class AddEmbeddingsTPGMElements implements PhysicalTPGMOperator {
    /**
     * Input embeddings
     */
    private final DataSet<EmbeddingTPGM> input;
    /**
     * Number of elements to add to the embedding.
     */
    private final int count;
    /**
     * Operator name used for Flink operator description
     */
    private String name;

    /**
     * New embeddings add operator
     *
     * @param input input embeddings
     * @param count number of elements to add
     */
    public AddEmbeddingsTPGMElements(DataSet<EmbeddingTPGM> input, int count) {
        this.input = input;
        this.count = count;
        this.setName("AddEmbeddingsElements");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return input.map(new AddEmbeddingTPGMElements(count));
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
