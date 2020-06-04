package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism.*;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

/**
 * Uses citibike data to test isomorphisms
 */
public class CBCypherTemporalPatternMatchingIsomorphismTest extends CBCypherTemporalPatternMatchingTest {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable data() {
        ArrayList<String[]> data = new ArrayList<>();
        data.addAll(new IsomorphismBeforeData().getData());
        data.addAll(new IsomorphismOverlapsData().getData());
        data.addAll(new IsomorphismAfterData().getData());
        data.addAll(new IsomorphismFromToData().getData());
        data.addAll(new IsomorphismBetweenData().getData());
        data.addAll(new IsomorphismPrecedesData().getData());
        data.addAll(new IsomorphismSucceedsData().getData());
        data.addAll(new IsomorphismAsOfData().getData());
        data.addAll(new IsomorphismComplexQueryData().getData());
        data.addAll(new IsomorphismContainsData().getData());
        data.addAll(new IsomorphismComparisonData().getData());
        data.addAll(new IsomorphismImmediatelyPrecedesTest().getData());
        data.addAll(new IsomorphismImmediatelySucceedsTest().getData());
        data.addAll(new IsomorphismEqualsTest().getData());
        data.addAll(new IsomorphismMinMaxTest().getData());
        data.addAll(new IsomorphismLongerThanData().getData());
        data.addAll(new IsomorphismShorterThanData().getData());
        data.addAll(new IsomorphismLengthAtLeastData().getData());
        data.addAll(new IsomorphismLengthAtMostData().getData());
        data.addAll(new IsomorphismMergeAndJoinData().getData());
        //data.addAll(new IsomorphismComplexQueryData().getData());
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
    public CBCypherTemporalPatternMatchingIsomorphismTest(String testName, String dataGraphPath, String queryGraph,
                                               String expectedGraphVariables, String expectedCollection) {
        super(testName, dataGraphPath, queryGraph, expectedGraphVariables, expectedCollection);
    }


    @Override
    public TemporalPatternMatching<TemporalGraphHead, TemporalGraph, TemporalGraphCollection>
    getImplementation(String queryGraph, boolean attachData) {
         //dummy value for dummy GraphStatistics
        int n = 42;
        TemporalGraphStatistics stats = null;
        try {
            stats = new BinningTemporalGraphStatisticsFactory()
                    .fromGraph(getTemporalGraphFromLoader(getLoader()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new CypherTemporalPatternMatching(queryGraph, attachData, MatchStrategy.ISOMORPHISM,
                MatchStrategy.ISOMORPHISM, stats, new CNFPostProcessing());
    }
}
