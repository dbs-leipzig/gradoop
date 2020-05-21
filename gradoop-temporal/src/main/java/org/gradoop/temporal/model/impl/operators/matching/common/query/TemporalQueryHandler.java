package org.gradoop.temporal.model.impl.operators.matching.common.query;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.exceptions.BailSyntaxErrorStrategy;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wraps a {@link GDLHandler} and adds functionality needed for query
 * processing during graph pattern matching.
 * Extension for temporal queries
 */
public class TemporalQueryHandler{
    /**
     * GDL handler
     */
    private final GDLHandler gdlHandler;

    /**
     * Time Literal representing the systime at the start of query processing
     */
    private final TimeLiteral now;

    /**
     * variable name in a global time selector in the query
     */
    public static final String GLOBAL_SELECTOR = TimeSelector.GLOBAL_SELECTOR;

    /**
     * Graph diameter
     */
    private Integer diameter;
    /**
     * Graph radius
     */
    private Integer radius;
    /**
     * Graph components
     */
    private Map<Integer, Set<String>> components;
    /**
     * Cache: vId --> Vertex with Id == vId
     */
    private Map<Long, Vertex> idToVertexCache;
    /**
     * Cache: eId --> Edge with Id == eId
     */
    private Map<Long, Edge> idToEdgeCache;
    /**
     * Cache: l -> Vertices with Label == l
     */
    private Map<String, Set<Vertex>> labelToVertexCache;
    /**
     * Cache: l -> Edges with Label = l
     */
    private Map<String, Set<Edge>> labelToEdgeCache;
    /**
     * Cache: vId -> Edges with Source Id == vId
     */
    private Map<Long, Set<Edge>> sourceIdToEdgeCache;
    /**
     * Cache: vId -> Edges with Target Id == vId
     */
    private Map<Long, Set<Edge>> targetIdToEdgeCache;

    /**
     * The edge cache of the GDL handler.
     */
    private Map<String, Edge> edgeCache;

    /**
     * The vertex cache of the GDL handler.
     */
    private Map<String, Vertex> vertexCache;

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
        now = new TimeLiteral("now");
        gdlHandler = new GDLHandler.Builder()
                .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
                .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
                .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
                .setErrorStrategy(new BailSyntaxErrorStrategy())
                .setProcessQuery(processQuery)
                .buildFromString(gdlString);
        edgeCache = gdlHandler.getEdgeCache(true, true);
        vertexCache = gdlHandler.getVertexCache(true, true);
    }

    /**
     * Returns all vertices in the query.
     *
     * @return vertices
     */
    public Collection<Vertex> getVertices() {
        return gdlHandler.getVertices();
    }

    /**
     * Returns all edges in the query.
     *
     * @return edges
     */
    public Collection<Edge> getEdges() {
        return gdlHandler.getEdges();
    }

    /**
     * Returns all variables contained in the pattern.
     *
     * @return all query variables
     */
    public Set<String> getAllVariables() {
        return Sets.union(getVertexVariables(), getEdgeVariables());
    }

    /**
     * Returns all vertex variables contained in the pattern.
     *
     * @return all vertex variables
     */
    public Set<String> getVertexVariables() {
        return vertexCache.keySet();
    }

    /**
     * Returns all edge variables contained in the pattern.
     *
     * @return all edge variables
     */
    public Set<String> getEdgeVariables() {
        return edgeCache.keySet();
    }

    /**
     * Returns all available predicates in Conjunctive Normal Form {@link TemporalCNF}. If there are no
     * predicated defined in the query, a CNF containing zero predicates is returned.
     *
     * @return predicates
     */
    public TemporalCNF getPredicates() {
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
     *//*
    public CNF getNonGlobalPredicates(){
        List<CNFElement> disj = getPredicates().getPredicates().stream()
                .filter(p -> !isGlobal(p))
                .collect(Collectors.toList());
        return new CNF(disj);
    }

    *//**
     * Returns a CNF of all disjunctions in the query that contain a global time selector.
     *
     * @return CNF of disjunctions containing global selectors
     *//*
    public CNF getGlobalPredicates(){
        List<CNFElement> disj = getPredicates().getPredicates().stream()
                .filter(p -> isGlobal(p))
                .collect(Collectors.toList());
        return new CNF(disj);
    }*/

    /**
     * Checks whether a single disjunction is global, i.e. contains a global time selector
     *
     * @param element the disjunction to check
     * @return true iff the disjunction contains a global time selector
     */
    private boolean isGlobal(CNFElementTPGM element){
        for(ComparisonExpressionTPGM comp: element.getPredicates()){
            if(!comp.isTemporal()){
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
     * Checks if the graph returns a single vertex and no edges (no loops).
     *
     * @return true, if single vertex graph
     */
    public boolean isSingleVertexGraph() {
        return getVertexCount() == 1 && getEdgeCount() == 0;
    }

    /**
     * Returns the number of vertices in the query graph.
     *
     * @return vertex count
     */
    public int getVertexCount() {
        return getVertices().size();
    }

    /**
     * Returns the number of edges in the query graph.
     *
     * @return edge count
     */
    public int getEdgeCount() {
        return getEdges().size();
    }

    /**
     * Returns the vertex associated with the given variable or {@code null} if the variable does
     * not exist. The variable can be either user-defined or auto-generated.
     *
     * @param variable query vertex variable
     * @return vertex or {@code null}
     */
    public Vertex getVertexByVariable(String variable) {
        return vertexCache.get(variable);
    }


    /**
     * Returns the Edge associated with the given variable or {@code null} if the variable does
     * not exist. The variable can be either user-defined or auto-generated.
     *
     * @param variable query edge variable
     * @return edge or {@code null}
     */
    public Edge getEdgeByVariable(String variable) {
        return edgeCache.get(variable);
    }

    /**
     * Returns the vertex associated with the given id or {@code null} if the
     * vertex does not exist.
     *
     * @param id vertex id
     * @return vertex or {@code null}
     */
    public Vertex getVertexById(Long id) {
        if (idToVertexCache == null) {
            idToVertexCache = initCache(getVertices(), Vertex::getId, Function.identity());
        }
        return idToVertexCache.get(id);
    }

    /**
     * Returns the edge associated with the given id or {@code null} if the
     * edge does not exist.
     *
     * @param id edge id
     * @return edge or {@code null}
     */
    public Edge getEdgeById(Long id) {
        if (idToEdgeCache == null) {
            idToEdgeCache = initCache(getEdges(), Edge::getId, Function.identity());
        }
        return idToEdgeCache.get(id);
    }

    /**
     * Initializes a cache for the given data where every key maps to exactly one element (injective).
     * Key selector will be called on every element to extract the caches key.
     * Value selector will be called on every element to extract the value.
     * Returns a cache of type
     * KT -> VT
     *
     * @param elements elements the cache will be build from
     * @param keySelector key selector function extraction cache keys from elements
     * @param valueSelector value selector function extraction cache values from elements
     * @param <EL> the element type
     * @param <KT> the cache key type
     * @param <VT> the cache value type
     * @return cache KT -> VT
     */
    private <EL, KT, VT> Map<KT, VT> initCache(Collection<EL> elements,
                                               Function<EL, KT> keySelector, Function<EL, VT> valueSelector) {

        return elements.stream().collect(Collectors.toMap(keySelector, valueSelector));
    }

    /**
     * Returns a mapping from edge variable to the corresponding source and target variables.
     *
     * @return mapping from edge variable to source/target variable
     */
    public Map<String, Pair<String, String>> getSourceTargetVariables() {
        return getEdges().stream()
                .map(e -> Pair.of(e.getVariable(), Pair
                        .of(getVertexById(e.getSourceVertexId()).getVariable(),
                                getVertexById(e.getTargetVertexId()).getVariable())))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    /**
     * Update variable names of vertices and edges with a generated variable name.
     * This will also update the vertex- and edge-caches of this handler.
     *
     * @param renameFunction The renaming function, mapping old to new variable names.
     */
    public void updateGeneratedVariableNames(Function<String, String> renameFunction) {
        Set<String> generatedEdgeVariables = gdlHandler.getEdgeCache(false, true).keySet();
        Set<String> generatedVertexVariables = gdlHandler.getVertexCache(false, true).keySet();
        Map<String, Vertex> newVertexCache = new HashMap<>();
        Map<String, Edge> newEdgeCache = new HashMap<>();
        // Update vertices.
        for (Vertex vertex : getVertices()) {
            final String variable = vertex.getVariable();
            if (generatedVertexVariables.contains(variable)) {
                vertex.setVariable(renameFunction.apply(variable));
            }
            newVertexCache.put(vertex.getVariable(), vertex);
        }
        // Update edges.
        for (Edge edge : getEdges()) {
            final String variable = edge.getVariable();
            if (generatedEdgeVariables.contains(variable)) {
                edge.setVariable(renameFunction.apply(variable));
            }
            newEdgeCache.put(edge.getVariable(), edge);
        }
        vertexCache = Collections.unmodifiableMap(newVertexCache);
        edgeCache = Collections.unmodifiableMap(newEdgeCache);
    }

    /**
     * Returns a mapping between the given variables (if existent) and the corresponding element
     * label.
     *
     * @param variables query variables
     * @return mapping between existing variables and their corresponding label
     */
    public Map<String, String> getLabelsForVariables(Collection<String> variables) {
        return variables.stream()
                .filter(var -> isEdge(var) || isVertex(var))
                .map(var -> {
                    if (isEdge(var)) {
                        return Pair.of(var, getEdgeByVariable(var).getLabel());
                    } else {
                        return Pair.of(var, getVertexByVariable(var).getLabel());
                    }
                }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    /**
     * Checks if the given variable points to a vertex.
     *
     * @param variable the elements variable
     * @return True if the variable points to a vertex
     */
    public boolean isVertex(String variable) {
        return vertexCache.containsKey(variable);
    }

    /**
     * Checks if the given variable points to an edge.
     *
     * @param variable the elements variable
     * @return True if the variable points to an edge
     */
    public boolean isEdge(String variable) {
        return edgeCache.containsKey(variable);
    }

}
