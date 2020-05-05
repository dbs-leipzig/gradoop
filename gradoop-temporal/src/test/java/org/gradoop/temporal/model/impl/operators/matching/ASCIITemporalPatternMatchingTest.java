package org.gradoop.temporal.model.impl.operators.matching;



import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.TestData;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;


import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CypherTemporalPatternMatching;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * base class for all temporal pattern matching tests that read ASCII-data as input db.
 */
@RunWith(Parameterized.class)
public abstract class ASCIITemporalPatternMatchingTest extends TemporalGradoopTestBase {

    /**
     * name of the test
     */
    protected final String testName;

    /**
     * the string describing the path to the DB graph to query
     */
    protected String dataGraphPath;

    /**
     * the query as GDL string
     */
    protected final String queryGraph;

    /**
     * expected graph variables (names) as comma-separated string
     */
    protected final String[] expectedGraphVariables;

    /**
     * expected graph collection as comma-separated GDLs
     */
    protected final String expectedCollection;

    /**
     * initializes a test with a data graph
     * @param testName name of the test
     * @param queryGraph the query graph as GDL-string
     * @param dataGraphPath path to data graph file
     * @param expectedGraphVariables expected graph variables (names) as comma-separated string
     * @param expectedCollection expected graph collection as comma-separated GDLs
     */
    public ASCIITemporalPatternMatchingTest(String testName, String dataGraphPath, String queryGraph,
                                            String expectedGraphVariables, String expectedCollection) {
        this.testName = testName;
        this.dataGraphPath = dataGraphPath;
        this.queryGraph = queryGraph;
        this.expectedGraphVariables = expectedGraphVariables.split(",");
        this.expectedCollection = expectedCollection;
    }

    /**
     * Yields a pattern matching implementation
     * @param queryGraph query graph as GDL string
     * @param attachData parameter of matching implementations
     * @return a pattern matching implementation
     */
    public abstract TemporalPatternMatching<
            TemporalGraphHead, TemporalGraph, TemporalGraphCollection> getImplementation(
                    String queryGraph, boolean attachData);

    @Test
    public void testGraphElementIdEquality() throws Exception {
        FlinkAsciiGraphLoader loader = getLoader();

        TemporalGraph db = getTemporalGraphFromLoader(loader);

        loader.appendToDatabaseFromString(expectedCollection);

        TemporalGraphCollection result = getImplementation(queryGraph, true).execute(db);
        TemporalGraphCollection expected = toTemporalGraphCollection(
                loader.getGraphCollectionByVariables(expectedGraphVariables));
        System.out.println(result.getGraphHeads().count());
        collectAndAssertTrue(result.equalsByGraphElementIds(expected));
    }

    @Test
    public void testGraphElementEquality() throws Exception {
        FlinkAsciiGraphLoader loader = getLoader();

        // initialize with data graph
        TemporalGraph db = getTemporalGraphFromLoader(loader);

        // append the expected result
        loader.appendToDatabaseFromString(expectedCollection);

        // execute and validate
        TemporalGraphCollection result = getImplementation(queryGraph, true).execute(db);
        TemporalGraphCollection expected = transformExpectedToTemporal(
                loader.getGraphCollectionByVariables(expectedGraphVariables));
        //System.out.println(result.getGraphHeads().count());
        collectAndAssertTrue(result.equalsByGraphElementData(expected));
    }

    @Test
    public void testVariableMappingExists() throws Exception {
        FlinkAsciiGraphLoader loader = getLoader();

        // initialize with data graph
        TemporalGraph db = getTemporalGraphFromLoader(loader);

        

        // append the expected result
        loader.appendToDatabaseFromString(expectedCollection);

        // execute and validate
        List<TemporalGraphHead> graphHeads = getImplementation(queryGraph, false)
                .execute(db).getGraphHeads().collect();


        for (TemporalGraphHead graphHead : graphHeads) {
            assertTrue(graphHead.hasProperty(PatternMatching.VARIABLE_MAPPING_KEY));
        }

    }

    @Test
    public void testGraphHeads() throws Exception {
        FlinkAsciiGraphLoader loader = getLoader();

        // initialize with data graph
        TemporalGraph db = getTemporalGraphFromLoader(loader);

        // append the expected result
        loader.appendToDatabaseFromString(expectedCollection);

        // execute and validate
        TemporalGraphCollection result = getImplementation(queryGraph, true)
                .execute(db);
        List<TemporalVertex> vertices = result.getVertices().collect();
        List<TemporalEdge> edges = result.getEdges().collect();
        List<TemporalElement> elements = new ArrayList<>(vertices);
        elements.addAll(edges);

        Set<GradoopId> graphIds = new HashSet<>();
        for(TemporalElement e: elements){
            Set<GradoopId> elementGraphs = e instanceof TemporalVertex ?
                    ((TemporalVertex)e).getGraphIds() :
                    ((TemporalEdge)e).getGraphIds();
            graphIds.addAll(elementGraphs);
        }

        // initialize with default time
        HashMap<GradoopId, Long[]> globalTime = new HashMap<>();
        for(GradoopId graphId: graphIds){
            globalTime.put(graphId, new Long[]{TemporalElement.DEFAULT_TIME_FROM, TemporalElement.DEFAULT_TIME_TO,
            TemporalElement.DEFAULT_TIME_FROM, TemporalElement.DEFAULT_TIME_TO});
        }

        // calculate expected global time data for each result graph head
        for(TemporalElement element: elements){
            Set<GradoopId> graphs = element instanceof TemporalVertex ?
                    ((TemporalVertex)element).getGraphIds() :
                    ((TemporalEdge)element).getGraphIds();
            for(GradoopId graph: graphs){
                Long newTxFrom = Math.max(element.getTxFrom(), globalTime.get(graph)[0]);
                Long newTxTo = Math.min(element.getTxTo(), globalTime.get(graph)[1]);
                Long newValFrom = Math.max(element.getTxFrom(), globalTime.get(graph)[2]);
                Long newValTo = Math.min(element.getTxTo(), globalTime.get(graph)[3]);
                if(newTxFrom > newTxTo){
                    newTxFrom = TemporalElement.DEFAULT_TIME_FROM;
                    newTxTo = TemporalElement.DEFAULT_TIME_TO;
                }
                if(newValFrom > newValTo){
                    newValFrom = TemporalElement.DEFAULT_TIME_FROM;
                    newValTo = TemporalElement.DEFAULT_TIME_TO;
                }
                globalTime.put(graph, new Long[]{newTxFrom, newTxTo, newValFrom, newValTo});
            }
        }

        // compare resulting global time data with expected
        System.out.println("Number of graphs: "+graphIds.size());
        for(GradoopId graphId: graphIds){
            TemporalGraphHead head = null;
            // not every returned logical graph corresponds to a matching result
            try{
                head = result.getGraph(graphId).getGraphHead().collect().get(0);
            }
            catch(IndexOutOfBoundsException e){
                continue;
            }
            Long[] expected = globalTime.get(head.getId());
            assertEquals(head.getTxFrom(), expected[0]);
            assertEquals(head.getTxTo(), expected[1]);
            assertEquals(head.getValidFrom(), expected[2]);
            assertEquals(head.getValidTo(), expected[3]);
        }

    }

    /**
     * In {@code testGraphElementEquality}, the {@code expected} variable might not be compatible
     * to the result graphs because EPGM graphs are transformed to TPGM graphs in
     * {@code getTemporalGraphFromLoader}. This method converts {@code expected} to the proper
     * format in order to make it comparable to {@code result}.
     *
     * @param graphCollectionByVariables the graph collection to transform to TPGM
     * @return transformed graph collection
     */
    protected abstract TemporalGraphCollection transformExpectedToTemporal(GraphCollection graphCollectionByVariables) throws Exception;

    /**
     * Creates a temporal graph from the input of a loader
     * @param loader a loader
     * @return temporal graph from the input of the loader
     */
    protected abstract TemporalGraph getTemporalGraphFromLoader(FlinkAsciiGraphLoader loader) throws Exception;

    /**
     * creates a loader for test data
     * @return loader for test data
     */
    protected abstract FlinkAsciiGraphLoader getLoader();
}