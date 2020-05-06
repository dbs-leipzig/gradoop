package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismImmediatelySucceedsTest implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmSucceedes_HOM_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE Interval(2013-06-01T00:18:11, 2020-05-05)" +
                                ".immediatelySucceeds(e.tx)"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmSucceedes_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE " +
                                "e.val.immediatelySucceeds(Interval(2013-06-01, 2013-06-01T00:04:22))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmSucceedes_HOM_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE Interval(2013-07-23, 2020-05-05)" +
                                ".immediatelySucceeds(a.val)"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // empty
        data.add(new String[]{
                "ImmSucceedes_HOM_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE Interval(2013-07-23T00:00:01, 2020-05-05).immediatelySucceeds(" +
                                "a.val)"
                ),
                "",
                ""
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        /*data.add(new String[]{
                "ImmSucceedes_HOM_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE val" +
                                ".immediatelySucceeds(Interval(1970-01-01, 2013-06-01T00:04:22)) " +
                                "AND Interval(2013-06-01T00:18:11,2020-05-05).immediatelySucceeds(tx)"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });*/

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        // 2.[(E 15 St & Irving Pl) -> (Washington Park))
        data.add(new String[]{
                "ImmSucceedes_HOM_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE Interval(2013-07-23, 2020-05-05).immediatelySucceeds(" +
                                "a.val.join(b.val))"
                ),
                "expected1,expected2",
                "expected1[(s14)-[e9]->(s15)], expected2[(s3)-[e3]->(s4)]"
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmSucceedes_HOM_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE Interval(2013-07-18, 2020-05-05).immediatelySucceeds(" +
                                "a.tx.merge(b.val))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        return data;
    }
}
