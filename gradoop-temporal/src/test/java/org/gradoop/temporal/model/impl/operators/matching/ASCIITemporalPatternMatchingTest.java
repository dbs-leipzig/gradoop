package org.gradoop.temporal.model.impl.operators.matching;



import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;


import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

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

        collectAndAssertTrue(result.equalsByGraphElementData(expected));
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