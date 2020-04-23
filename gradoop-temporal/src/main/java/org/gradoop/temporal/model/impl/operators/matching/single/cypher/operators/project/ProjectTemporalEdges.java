package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions.ProjectTemporalEdge;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.ArrayList;
import java.util.List;

/**
 * Projects a TPGM Edge by a set of properties.
 * <p>
 * {@code TemporalEdge -> EmbeddingTPGM(ProjectionEmbedding(Edge))}
 */
public class ProjectTemporalEdges implements PhysicalTPGMOperator {
    /**
     * Input edge
     */
    private final DataSet<TemporalEdge> input;
    /**
     * Names of the properties that will be kept in the projection
     */
    private final List<String> propertyKeys;
    /**
     * Indicates if the edges is loop
     */
    private final boolean isLoop;

    /**
     * Operator name used for Flink operator description
     */
    private String name;

    /**
     * Creates a new edge projection operator
     * @param input edges that should be projected
     * @param propertyKeys List of property names that will be kept in the projection
     * @param isLoop indicates if the edges is a loop
     */
    public ProjectTemporalEdges(DataSet<TemporalEdge> input, List<String> propertyKeys, boolean isLoop) {
        this.input = input;
        this.propertyKeys = propertyKeys;
        this.name = "ProjectEdges";
        this.isLoop = isLoop;
    }

    /**
     * Creates a new edge projection operator wih empty property list
     * Evaluate will return EmbeddingTPGM(IDEntry)
     *
     * @param input vertices that will be projected
     * @param isLoop indicates if the edges is a loop
     */
    public ProjectTemporalEdges(DataSet<TemporalEdge> input, boolean isLoop) {
        this(input, new ArrayList<>(), isLoop);
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return input
                .map(new ProjectTemporalEdge(propertyKeys, isLoop))
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
