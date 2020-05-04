package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.PostProcessor;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.debug.PrintEmbedding;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.debug.PrintEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions.ElementsFromEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add.AddEmbeddingsTPGMElements;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.ProjectTemporalEmbeddingsElements;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.planner.greedy.GreedyPlanner;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;

import java.util.Set;

import static org.gradoop.flink.model.impl.operators.matching.common.PostProcessor.extractGraphCollectionWithData;
import static org.gradoop.flink.model.impl.operators.matching.common.debug.Printer.log;

/**
 * Implementation of a query engine based on the Cypher graph query language.
 */
public class CypherTemporalPatternMatching
extends TemporalPatternMatching<TemporalGraphHead, TemporalGraph, TemporalGraphCollection> {

    /**
     * Logger
     */
    private static final Logger LOG = Logger.getLogger(CypherTemporalPatternMatching.class);
    /**
     * Construction pattern for result transformation.
     */
    private final String constructionPattern;
    /**
     * Morphism strategy for vertex mappings
     */
    private final MatchStrategy vertexStrategy;
    /**
     * Morphism strategy for edge mappings
     */
    private final MatchStrategy edgeStrategy;
    /**
     * Statistics about the data graph
     */
    private final GraphStatistics graphStatistics;

    /**
     * Instantiates a new operator.
     *
     * @param query           Cypher query string
     * @param attachData      true, if original data shall be attached to the result
     * @param vertexStrategy  morphism strategy for vertex mappings
     * @param edgeStrategy    morphism strategy for edge mappings
     * @param graphStatistics statistics about the data graph
     */
    public CypherTemporalPatternMatching(String query, boolean attachData,
                                         MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
                                         GraphStatistics graphStatistics) {
        this(query, null, attachData, vertexStrategy, edgeStrategy, graphStatistics);
    }

    /**
     * Instantiates a new operator.
     *
     * @param query               Cypher query string
     * @param constructionPattern Construction pattern
     * @param attachData          true, if original data shall be attached to the result
     * @param vertexStrategy      morphism strategy for vertex mappings
     * @param edgeStrategy        morphism strategy for edge mappings
     * @param graphStatistics     statistics about the data graph
     */
    public CypherTemporalPatternMatching(String query, String constructionPattern, boolean attachData,
                                 MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
        super(query, attachData, LOG);
        this.constructionPattern = constructionPattern;
        this.vertexStrategy = vertexStrategy;
        this.edgeStrategy = edgeStrategy;
        this.graphStatistics = graphStatistics;
    }

    @Override
    protected TemporalGraphCollection executeForVertex(TemporalGraph graph) {
        return executeForPattern(graph);
    }

    @Override
    protected TemporalGraphCollection executeForPattern(TemporalGraph graph) {
        // Query planning
        TemporalQueryHandler queryHandler = getQueryHandler();
        QueryPlan plan =
                new GreedyPlanner(graph, queryHandler, graphStatistics, vertexStrategy, edgeStrategy).plan()
                        .getQueryPlan();

        // Query execution
        DataSet<EmbeddingTPGM> embeddings = plan.execute();
        EmbeddingTPGMMetaData embeddingMetaData = plan.getRoot().getEmbeddingMetaData();

        embeddings =
                log(embeddings, new PrintEmbeddingTPGM(embeddingMetaData),
                        getVertexMapping(), getEdgeMapping());

        // Pattern construction (if necessary)
        DataSet<Element> finalElements = this.constructionPattern != null ?
                constructFinalElements(graph, embeddings, embeddingMetaData) :
                embeddings.flatMap(
                        new ElementsFromEmbeddingTPGM<TemporalGraphHead, TemporalVertex, TemporalEdge>(
                                graph.getFactory().getGraphHeadFactory(),
                                graph.getFactory().getVertexFactory(),
                                graph.getFactory().getEdgeFactory(),
                                embeddingMetaData,
                                queryHandler.getSourceTargetVariables()));

        // Post processing
        TemporalGraphCollection graphCollection = doAttachData() ?
                PostProcessor.<TemporalGraphHead, TemporalVertex,
                                        TemporalEdge, TemporalGraph, TemporalGraphCollection>
                        extractGraphCollectionWithData(
                                                finalElements, graph, true) :
                PostProcessor.<TemporalGraphHead, TemporalVertex,
                                                TemporalEdge, TemporalGraph, TemporalGraphCollection>
                        extractGraphCollection(finalElements, graph.getCollectionFactory(),
                                                true);

        return graphCollection;
    }

    /**
     * Method to construct final embedded elements
     *
     * @param graph               Used logical graph
     * @param embeddings          embeddings
     * @param embeddingMetaData   Meta information
     * @return                    New set of EmbeddingElements
     */
    private DataSet<Element> constructFinalElements(TemporalGraph graph, DataSet<EmbeddingTPGM> embeddings,
                                                    EmbeddingTPGMMetaData embeddingMetaData) {

        TemporalQueryHandler constructionPatternHandler = new TemporalQueryHandler(
                this.constructionPattern);
        constructionPatternHandler.updateGeneratedVariableNames(n -> "_" + n);

        Set<String> queryVars = Sets.newHashSet(embeddingMetaData.getVariables());
        Set<String> constructionVars = constructionPatternHandler.getAllVariables();
        Set<String> existingVars = intersection(queryVars, constructionVars).immutableCopy();
        Set<String> newVars = difference(constructionVars, queryVars).immutableCopy();

        EmbeddingTPGMMetaData newMetaData = computeNewMetaData(
                embeddingMetaData, constructionPatternHandler, existingVars, newVars);

        // project existing embedding elements to new embeddings
        ProjectTemporalEmbeddingsElements projectedEmbeddings =
                new ProjectTemporalEmbeddingsElements(embeddings, existingVars, embeddingMetaData, newMetaData);
        // add new embedding elements
        AddEmbeddingsTPGMElements addEmbeddingsElements =
                new AddEmbeddingsTPGMElements(projectedEmbeddings.evaluate(), newVars.size());

        return addEmbeddingsElements.evaluate().flatMap(
                new ElementsFromEmbeddingTPGM(
                        graph.getFactory().getGraphHeadFactory(),
                        graph.getFactory().getVertexFactory(),
                        graph.getFactory().getEdgeFactory(),
                        newMetaData,
                        constructionPatternHandler.getSourceTargetVariables(),
                        constructionPatternHandler.getLabelsForVariables(newVars)));
    }

    /**
     * Compute new meta information
     *
     * @param metaData              old meta information
     * @param returnPatternHandler  pattern handler
     * @param existingVariables     old variables
     * @param newVariables          new variables
     * @return                      new EmbeddingMetaData
     */
    private EmbeddingTPGMMetaData computeNewMetaData(EmbeddingTPGMMetaData metaData,
                                                 TemporalQueryHandler returnPatternHandler,
                                                     Set<String> existingVariables, Set<String> newVariables) {
        // update meta data
        EmbeddingTPGMMetaData newMetaData = new EmbeddingTPGMMetaData();

        // case 1: Filter existing embeddings based on return pattern
        for (String var : existingVariables) {
            newMetaData.setEntryColumn(var, metaData.getEntryType(var), newMetaData.getEntryCount());
        }

        // case 2: Add new vertices and edges
        for (String var : newVariables) {
            EmbeddingTPGMMetaData.EntryType type = returnPatternHandler.isEdge(var) ?
                    EmbeddingTPGMMetaData.EntryType.EDGE :
                    EmbeddingTPGMMetaData.EntryType.VERTEX;

            newMetaData.setEntryColumn(var, type, newMetaData.getEntryCount());
        }
        return newMetaData;
    }


}
