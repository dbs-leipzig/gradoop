package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism.*;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism.IsomorphismBeforeData;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

public class CBCypherTemporalPatternMatchingHomomorphismTest extends CBCypherTemporalPatternMatchingTest {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable data() {
        ArrayList<String[]> data = new ArrayList<>();
        //data.addAll(new HomomorphismBeforeData().getData());
        //data.addAll(new HomomorphismOverlapsData().getData());
        //data.addAll(new HomomorphismAfterData().getData());
        //data.addAll(new HomomorphismFromToData().getData());
        //data.addAll(new HomomorphismBetweenData().getData());
        //data.addAll(new HomomorphismPrecedesData().getData());
        data.addAll(new HomomorphismSucceedsData().getData());
        return data;
    }

    /**
     * initializes a test with a data graph
     * @param testName name of the test
     * @param queryGraph the query graph as GDL-string
     * @param dataGraphPath path to data graph file
     * @param expectedGraphVariables expected graph variables (names) as comma-separated string
     * @param expectedCollection expected graph collection as comma-separated GDLs
     */
    public CBCypherTemporalPatternMatchingHomomorphismTest(String testName, String dataGraphPath, String queryGraph,
                                                          String expectedGraphVariables, String expectedCollection) {
        super(testName, dataGraphPath, queryGraph, expectedGraphVariables, expectedCollection);
    }


    @Override
    public TemporalPatternMatching<TemporalGraphHead, TemporalGraph, TemporalGraphCollection>
    getImplementation(String queryGraph, boolean attachData) {
        // dummy value for dummy GraphStatistics
        int n = 42;
        GraphStatistics stats = new GraphStatistics(n,n,n,n);
        return new CypherTemporalPatternMatching(queryGraph, attachData, MatchStrategy.HOMOMORPHISM,
                MatchStrategy.HOMOMORPHISM, stats);
    }
}
