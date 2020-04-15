package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;

import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;

import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectTemporalVertex;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;
/**
 * Filters a set of {@link TemporalVertex} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link EmbeddingTPGM} that are specified in the
 * given {@code projectionPropertyKeys}.
 * <p>
 * {@code Vertex -> Embedding( [IdEntry(VertexId)], [PropertyEntry(v1),PropertyEntry(v2)], [TimeData(v)])}
 * <p>
 * Example:
 * <br>
 * Given a Vertex
 * {@code (0, "Person", {name:"Alice", age:23}, valid_from:123, valid_to:1234, tx_from:678, tx_to:6789)}, a predicate {@code "age = 23"} and
 * projection property keys [name, location] the operator creates an
 * {@link EmbeddingTPGM}:
 * <br>
 * {@code ([IdEntry(0)],[PropertyEntry(Alice),PropertyEntry(NULL)], [[123, 1234, 678, 6789]])}
 */
public class FilterAndProjectTemporalVertices implements PhysicalTPGMOperator {
    /**
     * Input vertices
     */
    private final DataSet<TemporalVertex> input;
    /**
     * Predicates in conjunctive normal form
     */
    private final CNF predicates;
    /**
     * Property keys used for projection
     */
    private final List<String> projectionPropertyKeys;

    /**
     * Operator name used for Flink operator description
     */
    private String name;

    /**
     * New vertex filter operator
     *
     * @param input Candidate vertices
     * @param predicates Predicates used to filter vertices
     * @param projectionPropertyKeys Property keys used for projection
     */
    public FilterAndProjectTemporalVertices(DataSet<TemporalVertex> input, CNF predicates,
                                    List<String> projectionPropertyKeys) {
        this.input = input;
        this.predicates = predicates;
        this.projectionPropertyKeys = projectionPropertyKeys;
        this.setName("FilterAndProjectVertices");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return input
                .flatMap(new FilterAndProjectTemporalVertex(predicates, projectionPropertyKeys))
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
