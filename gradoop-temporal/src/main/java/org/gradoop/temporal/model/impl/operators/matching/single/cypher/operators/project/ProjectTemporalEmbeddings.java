package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectTemporalEmbedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;
import java.util.stream.Collectors;

public class ProjectTemporalEmbeddings implements PhysicalTPGMOperator {
    /**
     * Input Embeddings
     */
    private final DataSet<EmbeddingTPGM> input;
    /**
     * Indices of all properties that will be kept in the projection
     */
    private final List<Integer> propertyWhiteList;

    /**
     * Operator name used for Flink operator description
     */
    private String name;

    /**
     * Creates a new embedding projection operator
     * @param input Embeddings that should be projected
     * @param propertyWhiteList property columns in the embedding that are taken over to the output
     */
    public ProjectTemporalEmbeddings(DataSet<EmbeddingTPGM> input, List<Integer> propertyWhiteList) {
        this.input = input;
        this.propertyWhiteList = propertyWhiteList.stream().sorted().collect(Collectors.toList());
        this.name = "ProjectEmbeddings";
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return input
                .map(new ProjectTemporalEmbedding(propertyWhiteList))
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
