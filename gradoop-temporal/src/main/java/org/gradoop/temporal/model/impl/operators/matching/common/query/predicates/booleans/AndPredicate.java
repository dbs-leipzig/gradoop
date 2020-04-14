package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.s1ck.gdl.model.predicates.booleans.And;

import java.util.Objects;

/**
 * Wraps an {@link org.s1ck.gdl.model.predicates.booleans.And} predicate
 * Extension for temporal predicates
 */
public class AndPredicate extends org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.AndPredicate {
    /**
     * Holds the wrapped predicate
     */
    private final And and;

    /**
     * Returns a new AndPredicate
     * @param and the predicate
     */
    public AndPredicate(And and) {
        super(and);
        this.and = and;
    }

    @Override
    public QueryPredicate getLhs() {
        return QueryPredicateFactory.createFrom(and.getArguments()[0]);
    }

    @Override
    public QueryPredicate getRhs() {
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
