package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectEmbeddingElements;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectTemporalEmbeddingElements;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Projects elements from the input {@link EmbeddingTPGM} into an output {@link EmbeddingTPGM} based on
 * a given set of variables.
 */
public class ProjectTemporalEmbeddingsElements implements PhysicalTPGMOperator {
    /**
     * Input graph elements
     */
    private final DataSet<EmbeddingTPGM> input;
    /**
     * Return pattern variables that already exist in pattern matching query
     */
    private final Set<String> projectionVariables;
    /**
     * Meta data for input embeddings
     */
    private final EmbeddingTPGMMetaData inputMetaData;
    /**
     * Meta data for output embeddings
     */
    private final EmbeddingTPGMMetaData outputMetaData;
    /**
     * Operator name used for Flink operator description
     */
    private String name;

    /**
     * New embeddings filter operator
     *
     * @param input               input embeddings
     * @param projectionVariables projection variables
     * @param inputMetaData       input meta data
     * @param outputMetaData      output meta data
     */
    public ProjectTemporalEmbeddingsElements(DataSet<EmbeddingTPGM> input,
                                             Set<String> projectionVariables,
                                             EmbeddingTPGMMetaData inputMetaData,
                                             EmbeddingTPGMMetaData outputMetaData) {
        this.input = input;
        this.projectionVariables = projectionVariables;
        this.inputMetaData = inputMetaData;
        this.outputMetaData = outputMetaData;
        this.setName("ProjectEmbeddingsElements");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        Map<Integer, Integer> projection = projectionVariables.stream()
                .collect(Collectors.toMap(inputMetaData::getEntryColumn, outputMetaData::getEntryColumn));

        return input.map(new ProjectTemporalEmbeddingElements(projection));
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
