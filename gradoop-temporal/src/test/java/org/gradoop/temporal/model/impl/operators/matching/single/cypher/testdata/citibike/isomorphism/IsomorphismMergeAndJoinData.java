package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismMergeAndJoinData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();

        //1.[(Fulton St) (Shevchenko Pl)
        data.add(new String[]{
                "MergeJoin_ISO_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a) (b) WHERE a.id>b.id AND a.val.merge(b.val).overlaps(" +
                                "Interval(2013-07-28T12:00, 2013-07-30))"),
                "expected1",
                "expected1[(s20)(s25)]"
        });

        // empty
        data.add(new String[]{
                "MergeJoin_ISO_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a) WHERE NOT a.val.merge(a.val).contains(a.val.join(a.val))"
                ),
                "",
                ""
        });
        // 1.[(Murray St & West St) -> (Shevchenko Pl)]
        data.add(new String[]{
                "MergeJoin_ISO_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.tx.join(b.tx).contains(Interval(" +
                                " 2013-05-12,2013-07-28))"
                ),
                "expected1",
                "expected1[(s24)-[e15]->(s25)]"
        });

        // do not merge/join when no overlap
        // 1. [(Broadway & W 24 St) -[e1]-> (9 Ave & W 18)]

        data.add(new String[]{
                "MergeJoin_ISO_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.id=444 AND Interval(1970-01-01,1970-01-02).precedes(" +
                                "e.val.join(Interval(2013-06-01T00:11:40,2017-01-01)))"
                ),
                "expected1",
                "expected1[(s0)-[e1]->(s1)]"
        });
        // 1. [(Broadway & W 24 St) -[e1]-> (9 Ave & W 18)]

        data.add(new String[]{
                "MergeJoin_ISO_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.id=444 AND Interval(1970-01-01,1970-01-02).precedes(" +
                                "e.val.merge(Interval(2013-06-01T00:11:40,2017-01-01)))"
                ),
                "expected1",
                "expected1[(s0)-[e1]->(s1)]"
        });
        // 1. [(Broadway & W24 St) -[e0]-> (9 Ave & W 18 St)]
        // 2. [(Broadway & W24 St) -[e1]-> (9 Ave & W 18 St)]
        data.add(new String[]{
                "MergeJoin_ISO_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE val.join(a.val).overlaps(Interval(2013-05-01, 2013-05-11))"
                ),
                "expected1,expected2",
                "expected1[(s0)-[e0]->(s1)],expected2[(s0)-[e1]->(s1)]"
        });

        // 1.[(Murray St & West St) -> (Shevchenko Pl)]
        data.add(new String[]{
                "MergeJoin_ISO_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.tx.join(" +
                                "Interval(MIN(b.tx_from, e.tx_from), MAX(b.tx_to, e.tx_to)))" +
                                ".contains(Interval(2013-05-12,2013-07-28))"
                ),
                "expected1",
                "expected1[(s24)-[e15]->(s25)]"
        });

        return data;
    }
}
