package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectTemporalTriple;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.TripleTPGM;

import java.util.List;
import java.util.Map;

/**
 * Filters a set of {@link TripleTPGM} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link EmbeddingTPGM} that are specified in the
 * given {@code projectionPropertyKeys}.
 * <p>
 * {@code Triple -> EmbeddingTPGM( [PropertyEntry(SourceVertexId)], [PropertyEntry(EdgeId)], PropertyEntry
 * (TargetVertexId)])}
 * <p>
 * Example:
 * <br>
 * Given a Triple
 * {@code (
 *   p1: SourceVertex(0, "Person", {name:"Alice", age:23, location: "Sweden"}),
 *   e1: Edge(1, "knows", {}),
 *   p2: TargetVertex(2, "Person", {name:"Bob", age:23})
 * )},
 * a predicate {@code "p1.name = "Alice" AND p1.age <= p2.age"} and
 * projection property keys {@code p1: [name, location], e1: [], p2: [name, location]} the operator creates
 * an {@link EmbeddingTPGM}:
 * <br>
 * {@code ([IdEntry(0),IdEntry(1),IdEntry(2)],[PropertyEntry("Alice"),PropertyEntry("Sweden"),
 * PropertyEntry("Bob"),PropertyEntry(NULL)])}
 */
public class FilterAndProjectTemporalTriples implements PhysicalTPGMOperator {
    /**
     * Input vertices
     */
    private final DataSet<TripleTPGM> input;
    /**
     * Variable assigned to the source vertex
     */
    private final String sourceVariable;
    /**
     * Variable assigned to the edge
     */
    private final String edgeVariable;
    /**
     * Variable assigned to the target vertex
     */
    private final String targetVariable;
    /**
     * Predicates in conjunctive normal form
     */
    private final CNF predicates;
    /**
     * Property keys used for projection
     */
    private final Map<String, List<String>> projectionPropertyKeys;
    /**
     * Match Strategy used for Vertices
     */
    private final MatchStrategy vertexMatchStrategy;

    /**
     * Operator name used for Flink operator description
     */
    private String name;

    /**
     * New vertex filter operator
     *
     * @param input Candidate vertices
     * @param sourceVariable Variable assigned to the vertex
     * @param edgeVariable Variable assigned to the vertex
     * @param targetVariable Variable assigned to the vertex
     * @param predicates Predicates used to filter vertices
     * @param projectionPropertyKeys Property keys used for projection
     * @param vertexMatchStrategy Vertex match strategy
     */
    public FilterAndProjectTemporalTriples(DataSet<TripleTPGM> input, String sourceVariable, String edgeVariable,
                                   String targetVariable, CNF predicates, Map<String, List<String>> projectionPropertyKeys,
                                   MatchStrategy vertexMatchStrategy) {
        this.input = input;
        this.sourceVariable = sourceVariable;
        this.edgeVariable = edgeVariable;
        this.targetVariable = targetVariable;
        this.predicates = predicates;
        this.projectionPropertyKeys = projectionPropertyKeys;
        this.vertexMatchStrategy = vertexMatchStrategy;
        this.setName("FilterAndProjectTemporalTriples");
    }

    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        return input.flatMap(
                new FilterAndProjectTemporalTriple(
                        sourceVariable,
                        edgeVariable,
                        targetVariable,
                        predicates,
                        projectionPropertyKeys,
                        vertexMatchStrategy
                )
        ).name(getName());
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