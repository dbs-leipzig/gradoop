package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryPredicateTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.s1ck.gdl.model.predicates.booleans.Or;

import java.util.Objects;

/**
 * Wraps an {@link org.s1ck.gdl.model.predicates.booleans.Or} predicate
 */
public class OrPredicate extends QueryPredicateTPGM {
    /**
     * Holds the wrapped or predicate
     */
    private final Or or;

    /**
     * Creates a new or wrapper
     * @param or the wrapped or predicate
     */
    public OrPredicate(Or or) {
        this.or = or;
    }

    /**
     * Converts the predicate into conjunctive normal form
     * @return predicate in cnf
     */
    public TemporalCNF asCNF() {
        return getLhs().asCNF().or(getRhs().asCNF());
    }

    /**
     * Returns the left hand side predicate
     * @return the left hand side predicate
     */
    public QueryPredicateTPGM getLhs() {
        return QueryPredicateFactory.createFrom(or.getArguments()[0]);
    }

    /**
     * Returns the right hand side predicate
     * @return the right hand side predicate
     */
    public QueryPredicateTPGM getRhs() {
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
