package org.gradoop.temporal.model.impl.operators.matching.common.query;

import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
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

import java.util.*;
import java.util.stream.Collectors;

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
     * Creates a new query handler that postprocesses the query, i.e. reduces it to simple comparisons.
     *
     * @param gdlString GDL query string
     */
    public TemporalQueryHandler(String gdlString) {
        this(gdlString, true);
    }

    /**
     * Creates a new query handler.
     *
     * @param gdlString GDL query string
     * @param processQuery flag to indicate whether query should be postprocessed, i.e. reduced to simple
     *                     comparisons
     */
    public TemporalQueryHandler(String gdlString, boolean processQuery){
        super(gdlString);
        now = new TimeLiteral("now");
        gdlHandler = new GDLHandler.Builder()
                .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
                .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
                .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
                .setErrorStrategy(new BailSyntaxErrorStrategy())
                .setProcessQuery(processQuery)
                .buildFromString(gdlString);
    }

    @Override
    public CNF getPredicates() {
        System.out.println(gdlHandler.getPredicates());
        if (gdlHandler.getPredicates().isPresent()) {
            Predicate predicate = gdlHandler.getPredicates().get();
            predicate = preprocessPredicate(predicate);
            return QueryPredicateFactory.createFrom(predicate).asCNF();
        } else {
            return QueryPredicateFactory.createFrom(preprocessPredicate(null)).asCNF();
        }
    }

    /**
     * Returns a CNF of all disjunctions in the query that do not contain a global time selector.
     *
     * @return non-global CNF
     */
    public CNF getNonGlobalPredicates(){
        List<CNFElement> disj = getPredicates().getPredicates().stream()
                .filter(p -> !isGlobal(p))
                .collect(Collectors.toList());
        return new CNF(disj);
    }

    /**
     * Returns a CNF of all disjunctions in the query that contain a global time selector.
     *
     * @return CNF of disjunctions containing global selectors
     */
    public CNF getGlobalPredicates(){
        List<CNFElement> disj = getPredicates().getPredicates().stream()
                .filter(p -> isGlobal(p))
                .collect(Collectors.toList());
        return new CNF(disj);
    }

    /**
     * Checks whether a single disjunction is global, i.e. contains a global time selector
     *
     * @param element the disjunction to check
     * @return true iff the disjunction contains a global time selector
     */
    private boolean isGlobal(CNFElement element){
        for(ComparisonExpression comp: element.getPredicates()){
            comp = (ComparisonExpressionTPGM) comp;
            if(!((ComparisonExpressionTPGM) comp).isTemporal()){
                return false;
            }
            else{
                if(((TemporalComparable)comp.getLhs()).isGlobal() ||
                ((TemporalComparable)comp.getRhs()).isGlobal()){
                    return true;
                }
            }
        }
        return false;
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
            ArrayList<String> vars = new ArrayList<>(gdlHandler.getEdgeCache(true, true).keySet());
            vars.addAll(gdlHandler.getVertexCache(true, true).keySet());
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
