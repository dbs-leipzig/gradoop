package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.s1ck.gdl.model.predicates.booleans.Or;

import java.util.Objects;

/**
 * Wraps an {@link org.s1ck.gdl.model.predicates.booleans.Or} predicate
 * Extension for temporal predicates
 */
public class OrPredicate extends org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.OrPredicate {
    /**
     * Holds the wrapped or predicate
     */
    private final Or or;

    /**
     * Creates a new or wrapper
     * @param or the wrapped or predicate
     */
    public OrPredicate(Or or) {
        super(or);
        this.or = or;
    }

    @Override
    public QueryPredicate getLhs() {
        return QueryPredicateFactory.createFrom(or.getArguments()[0]);
    }

    @Override
    public QueryPredicate getRhs() {
        return QueryPredicateFactory.createFrom(or.getArguments()[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OrPredicate orPredicateWrapper = (OrPredicate) o;

        return Objects.equals(or, orPredicateWrapper.or);
    }

    @Override
    public int hashCode() {
        return or != null ? or.hashCode() : 0;
    }
}
