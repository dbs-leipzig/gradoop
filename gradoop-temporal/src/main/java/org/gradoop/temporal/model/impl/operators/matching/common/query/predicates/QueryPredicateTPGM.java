package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.AndPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.NotPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.OrPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.XorPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Xor;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.io.Serializable;

/**
 * Wrapps a Predicate
 */
public abstract class QueryPredicateTPGM implements Serializable {


    /**
     * Generic wrapper function to createFrom a given predicate
     * @param predicate the predicate to be warpped
     * @return the wrapped predicate
     */
    public static QueryPredicateTPGM createFrom(Predicate predicate) {
        if (predicate.getClass() == And.class) {
            return new AndPredicate((And) predicate);

        } else if (predicate.getClass() == Or.class) {
            return new OrPredicate((Or) predicate);

        } else if (predicate.getClass() == Xor.class) {
            return new XorPredicate((Xor) predicate);

        } else if (predicate.getClass() == Not.class) {
            return new NotPredicate((Not) predicate);

        } else if (predicate.getClass() == Comparison.class) {
            return new ComparisonExpressionTPGM((Comparison) predicate);
        } else {
            throw new IllegalArgumentException(predicate.getClass() + " is not a GDL Predicate");
        }
    }
    /**
     * Converts the predicate into Conjunctive Normal Form
     * @return the predicate in CNF
     */
    public abstract TemporalCNF asCNF();
}
