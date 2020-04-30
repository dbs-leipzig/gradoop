package org.gradoop.temporal.model.impl.operators.matching.common.query;

import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.exceptions.BailSyntaxErrorStrategy;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link GDLHandler} and adds functionality needed for query
 * processing during graph pattern matching.
 * Extension for temporal queries
 */
public class TemporalQueryHandler extends QueryHandler {
    /**
     * GDL handler
     */
    private final GDLHandler gdlHandler;

    /**
     * Time Literal representing the systime at the start of query processing
     */
    private final TimeLiteral now;

    /**
     * Creates a new query handler.
     *
     * @param gdlString GDL query string
     */
    public TemporalQueryHandler(String gdlString) {
        super(gdlString);
        now = new TimeLiteral("now");
        gdlHandler = new GDLHandler.Builder()
                .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
                .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
                .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
                .setErrorStrategy(new BailSyntaxErrorStrategy())
                .buildFromString(gdlString);
    }

    @Override
    public CNF getPredicates() {
        if (gdlHandler.getPredicates().isPresent()) {
            Predicate predicate = gdlHandler.getPredicates().get();
            predicate = preprocessPredicate(predicate);
            return QueryPredicateFactory.createFrom(predicate).asCNF();
        } else {
            return QueryPredicateFactory.createFrom(preprocessPredicate(null)).asCNF();
        }
    }

    /**
     * Pipeline for preprocessing query predicates. Currently, only a {@link #defaultAsOfExpansion}
     * is done.
     * @param predicate hte predicate to preprocess
     * @return preprocessed predicate
     */
    private Predicate preprocessPredicate(Predicate predicate){
        return defaultAsOfExpansion(predicate);
    }

    /**
     * Preprocessing function for predicates. Adds v.asOf(now) constraints for every
     * query graph element v iff no constraint on any tx_to value is contained in the
     * predicate.
     * @param predicate predicate to augment with asOf(now) predicates
     * @return predicate with v.asOf(now) constraints for every
     * query graph element v iff no constraint on any tx_to value is contained in the
     *  predicate (else input predicate is returned).
     */
    private Predicate defaultAsOfExpansion(Predicate predicate){
        if(predicate!=null && predicate.containsSelectorType(TimeSelector.TimeField.TX_TO)){
            // no default asOfs if a constraint on any tx_to value is contained
            return predicate;
        }
        else{
            // add v.asOf(now) for every element in the query
            ArrayList<String> vars = new ArrayList<>(gdlHandler.getEdgeCache().keySet());
            vars.addAll(gdlHandler.getVertexCache().keySet());
            And asOf0 = new And(
                    new Comparison(
                            new TimeSelector(vars.get(0), TimeSelector.TimeField.TX_FROM),
                            Comparator.LTE, now),
                    new Comparison(
                            new TimeSelector(vars.get(0), TimeSelector.TimeField.TX_TO),
                            Comparator.GTE, now)
            );
            if(predicate == null){
                predicate = asOf0;
            }
            else{
                predicate = new And(predicate, asOf0);
            }
            for(int i=1; i<vars.size(); i++){
                String v = vars.get(i);
                And asOf = new And(
                        new Comparison(
                                new TimeSelector(v, TimeSelector.TimeField.TX_FROM),
                                Comparator.LTE, now),
                        new Comparison(
                                new TimeSelector(v, TimeSelector.TimeField.TX_TO),
                                Comparator.GTE, now)
                        );
                predicate = new And(predicate, asOf);
            }
            return predicate;
        }
    }

    /**
     * Returns the TimeLiteral representing the systime at the start of query processing
     * @return TimeLiteral representing the systime at the start of query processing
     */
    public TimeLiteral getNow(){
        return now;
    }

    /**
     * Returns the expansion conditions for a path.
     * @param startVariable
     * @return
     */
    public ExpansionCriteria getExpansionCondition(String startVariable) {
        //TODO implement
        return new ExpansionCriteria();
    }
}
