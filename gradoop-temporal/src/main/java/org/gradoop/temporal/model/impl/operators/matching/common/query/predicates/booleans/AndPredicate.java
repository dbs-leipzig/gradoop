package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryPredicateTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.s1ck.gdl.model.predicates.booleans.And;

import java.util.Objects;

/**
 * Wraps an {@link org.s1ck.gdl.model.predicates.booleans.And} predicate
 */
public class AndPredicate extends QueryPredicateTPGM {
    /**
     * Holds the wrapped predicate
     */
    private final And and;

    /**
     * Returns a new AndPredicate
     * @param and the predicate
     */
    public AndPredicate(And and) {
        this.and = and;
    }

    /**
     * Converts the predicate into conjunctive normal form
     * @return predicate in CNF
     */
    public TemporalCNF asCNF() {
        return getLhs().asCNF()
                .and(getRhs().asCNF());
    }

    /**
     * Retruns the wrapped left hand side predicate
     * @return the left hand side
     */
    public QueryPredicateTPGM getLhs() {
        return QueryPredicateFactory.createFrom(and.getArguments()[0]);
    }

    /**
     * Retruns the wrapped right hand side predicate
     * @return the right hand side
     */
    public QueryPredicateTPGM getRhs() {
        return QueryPredicateFactory.createFrom(and.getArguments()[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AndPredicate that = (AndPredicate) o;

        return Objects.equals(and, that.and);
    }

    @Override
    public int hashCode() {
        return and != null ? and.hashCode() : 0;
    }
}
