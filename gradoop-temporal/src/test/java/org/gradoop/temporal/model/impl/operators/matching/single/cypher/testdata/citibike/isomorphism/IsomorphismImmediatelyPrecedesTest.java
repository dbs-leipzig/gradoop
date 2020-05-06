package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

/**
 * same as for homomorphism
 */
public class IsomorphismImmediatelyPrecedesTest implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmPrecedes_ISO_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE e.tx.immediatelyPrecedes(" +
                                "Interval(2013-06-01T00:18:11, 2020-05-05))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmPrecedes_ISO_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE Interval(2013-06-01, 2013-06-01T00:04:22)" +
                                ".immediatelyPrecedes(e.val)"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmPrecedes_ISO_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.immediatelyPrecedes(" +
                                "Interval(2013-07-23, 2020-05-05))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // empty
        data.add(new String[]{
                "ImmPrecedes_ISO_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.immediatelyPrecedes(" +
                                "Interval(2013-07-23T00:00:01, 2020-05-05))"
                ),
                "",
                ""
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
//        data.add(new String[]{
//                "ImmPrecedes_ISO_5_default_citibike",
//                CBCypherTemporalPatternMatchingTest.defaultData,
//                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
//                        "MATCH (a)-[e]->(b) WHERE Interval(1970-01-01, 2013-06-01T00:04:22)" +
//                                ".immediatelyPrecedes(" +
//                                "val) AND tx.immediatelyPrecedes(Interval(2013-06-01T00:18:11,2020-05-05))"
//                ),
//                "expected1",
//                "expected1[(s14)-[e9]->(s15)]"
//        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        // 2.[(E 15 St & Irving Pl) -> (Washington Park))
        data.add(new String[]{
                "ImmPrecedes_ISO_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).immediatelyPrecedes(" +
                                "Interval(2013-07-23, 2020-05-05))"
                ),
                "expected1,expected2",
                "expected1[(s14)-[e9]->(s15)], expected2[(s3)-[e3]->(s4)]"
        });

        // 1.[(9 Ave & W14) -> (Mercer St & Spring St)]
        data.add(new String[]{
                "ImmPrecedes_ISO_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.tx.merge(b.val).immediatelyPrecedes(" +
                                "Interval(2013-07-18, 2020-05-05))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        return data;
    }
}
