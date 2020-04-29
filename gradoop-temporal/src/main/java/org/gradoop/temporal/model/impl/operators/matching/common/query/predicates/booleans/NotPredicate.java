package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.Objects;

/**
 * Wraps an {@link org.s1ck.gdl.model.predicates.booleans.Not} predicate
 * Extension for temporal predicates
 */
public class NotPredicate extends org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.NotPredicate {
    /**
     * Holds the wrapped not predicate
     */
    private final Not not;

    /**
     * Create a new wrapper
     * @param not the wrapped not predicate
     */
    public NotPredicate(Not not) {
        super(not);
        this.not = not;
    }

    @Override
    public CNF asCNF() {
        Predicate expression = not.getArguments()[0];

        if (expression.getClass() == Comparison.class) {
            CNF cnf = new CNF();
            CNFElement cnfElement = new CNFElement();
            cnfElement.addPredicate(new ComparisonExpressionTPGM(invertComparison((Comparison) expression)));
            cnf.addPredicate(cnfElement);
            return cnf;

        } else if (expression.getClass() == Not.class) {
            return QueryPredicateFactory.createFrom(expression.getArguments()[0]).asCNF();

        } else if (expression.getClass() == And.class) {
            Predicate[] otherArguments = expression.getArguments();
            Or or = new Or(
                    new Not(otherArguments[0]),
                    new Not(otherArguments[1])
            );
            return QueryPredicateFactory.createFrom(or).asCNF();

        } else if (expression.getClass() == Or.class) {
            Predicate[] otherArguments = expression.getArguments();
            And and = new And(
                    new Not(otherArguments[0]),
                    new Not(otherArguments[1])
            );

            return QueryPredicateFactory.createFrom(and).asCNF();

        } else {
            Predicate[] otherArguments = expression.getArguments();
            Or or = new Or(
                    new And(
                            otherArguments[0],
                            otherArguments[1]),
                    new And(
                            new Not(otherArguments[0]),
                            new Not(otherArguments[1]))
            );

            return QueryPredicateFactory.createFrom(or).asCNF();
        }
    }

    /**
     * Invert a comparison
     * eg NOT(a > b) == (a <= b)
     * @param comparison the comparison that will be inverted
     * @return inverted comparison
     */
    private Comparison invertComparison(
            Comparison comparison) {
        ComparableExpression[] arguments = comparison.getComparableExpressions();
        Comparator op = comparison.getComparator();

        return new Comparison(
                arguments[0],
                op.getInverse(),
                arguments[1]
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NotPredicate that = (NotPredicate) o;

        return Objects.equals(not, that.not);
    }

    @Override
    public int hashCode() {
        return not != null ? not.hashCode() : 0;
    }
}
