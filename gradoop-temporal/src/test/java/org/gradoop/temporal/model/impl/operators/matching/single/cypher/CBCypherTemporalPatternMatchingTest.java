package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.ASCIITemporalPatternMatchingTest;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism.IsomorphismBeforeData;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Uses citibike data to test matches. Base class for tests on isomorphism and homomorphism.
 */
public abstract class CBCypherTemporalPatternMatchingTest extends ASCIITemporalPatternMatchingTest {

    /**
     * Path to the default GDL file.
     */
    public static final String defaultData = "src/test/resources/data/patternmatchingtest/citibikesample";

    /*@Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable data() {
        ArrayList<String[]> data = new ArrayList<>();
        data.addAll(new IsomorphismBeforeData().getData());
        return data;
    }*/



    /**
     * Set the edge's {@code valid_from} and {@code tx_from} according to the {@code start}
     * property and the edge's {@code valid_to} and {@code tx_to} according to the
     * {@code end} property. Both properties are retained.
     */
    private final MapFunction<TemporalEdge, TemporalEdge> edgeTransform = new
            MapFunction<TemporalEdge, TemporalEdge>() {
        @Override
        public TemporalEdge map(TemporalEdge value) throws Exception {
            long start = value.getPropertyValue("start").getLong();
            long end = value.getPropertyValue("end").getLong();
            value.setValidTime(new Tuple2<>(start, end));
            value.setTransactionTime(value.getValidTime());
            //value.removeProperty("start");
            //value.removeProperty("end");
            return value;
        }
    };

    /**
     * initializes a test with a data graph
     * @param testName name of the test
     * @param queryGraph the query graph as GDL-string
     * @param dataGraphPath path to data graph file
     * @param expectedGraphVariables expected graph variables (names) as comma-separated string
     * @param expectedCollection expected graph collection as comma-separated GDLs
     */
    public CBCypherTemporalPatternMatchingTest(String testName, String dataGraphPath, String queryGraph,
                                               String expectedGraphVariables, String expectedCollection) {
        super(testName, dataGraphPath, queryGraph, expectedGraphVariables, expectedCollection);
    }

    @Override
    protected TemporalGraphCollection transformExpectedToTemporal(GraphCollection gc) throws Exception {
        //transform edges
        TemporalGraphCollection tgc = toTemporalGraphCollection(gc);
        DataSet<TemporalEdge> newEdges = tgc.getEdges().map(edgeTransform);
        tgc = tgc.getFactory().fromDataSets(tgc.getGraphHeads(), tgc.getVertices(), newEdges);
        return tgc;
    }

    /*@Override
    public TemporalPatternMatching<TemporalGraphHead, TemporalGraph, TemporalGraphCollection>
    getImplementation(String queryGraph, boolean attachData) {
        // dummy value for dummy GraphStatistics
        int n = 42;
        GraphStatistics stats = new GraphStatistics(n,n,n,n);
        return new CypherTemporalPatternMatching(queryGraph, attachData, MatchStrategy.ISOMORPHISM,
                MatchStrategy.ISOMORPHISM, stats);
    }*/

    @Override
    protected TemporalGraph getTemporalGraphFromLoader(FlinkAsciiGraphLoader loader) throws Exception {
        try {
            loader.initDatabaseFromFile(dataGraphPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LogicalGraph g = loader.getLogicalGraph();
        //new DOTDataSink("src/test/resources/data/patternmatchingtest/citibikesample.dot",true).write(g, true);
        return transformToTemporalGraph(g);
    }

    /**
     * Given a logical graph, this method transforms it to a temporal graph.
     * {@code start} and {@code end} values are extracted from the edges (= "trips")
     * and used to set {@code valid_from}/{@code tx_from} and {@code valid_to}/{@code tx_to}
     * values.
     *
     * @param g the logical graph to transform
     * @return logical graph transformed to temporal graph
     */
    private TemporalGraph transformToTemporalGraph(LogicalGraph g) throws Exception {
        TemporalGraph tg = toTemporalGraph(g);
        List<TemporalEdge> newEdges = tg.getEdges().map(edgeTransform).collect();
        return tg.getFactory().fromCollections(tg.getVertices().collect(),
                newEdges);
    }

    @Override
    protected FlinkAsciiGraphLoader getLoader() {
        return new FlinkAsciiGraphLoader(getConfig());
    }
}
