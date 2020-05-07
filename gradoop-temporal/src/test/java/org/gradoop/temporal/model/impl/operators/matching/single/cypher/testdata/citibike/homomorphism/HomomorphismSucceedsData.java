package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismSucceedsData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        /*
         * 1.[(E 15 St & Irving)->(Washington Park)  (Henry St & Grand St)->(S5 Pl & S 5 St)]
         */
        data.add(new String[]{
                "Succeeds_HOM_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH ()-[e1]->() ()-[e2]->(a) WHERE a.id=532 AND e1.edgeId=3" +
                        " AND e2.val.succeeds(e1.val)"),
                "expected1",
                "expected1[(s3)-[e3]->(s4) (s18)-[e11]->(s9)]"
        });
        /*
         * 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         * 2.[(E 20 St & Park Ave) -> (E 20 St & Park Ave)]
         * 3.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "Succeeds_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE e.tx" +
                        ".succeeds(Interval(2013-06-01T00:00:00, 2013-06-01T00:07:00))"),
                "expected1,expected2,expected3",
                "expected1[(s21)-[e19]->(s11)], expected2[(s27)-[e17]->(s27)], " +
                        "expected3[(s28)-[e18]->(s29)]"
        });
        /*
         * same as above, but now testing call from timestamp
         * 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         * 2.[(E 20 St & Park Ave) -> (E 20 St & Park Ave)]
         * 3.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "Succeeds_HOM_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE e.tx_from.succeeds(Interval(2013-06-01T00:00:00," +
                        "2013-06-01T00:07:00))"),
                "expected1,expected2,expected3",
                "expected1[(s21)-[e19]->(s11)], expected2[(s27)-[e17]->(s27)], " +
                        "expected3[(s28)-[e18]->(s29)]"
        });
        /*
         * 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
         * 2.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
         */
        data.add(new String[]{
                "Succeeds_HOM_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE a.id=444 AND " +
                        "Interval(2013-06-01T00:11:41, 2013-06-01T00:11:50)" +
                        ".succeeds(e.val)"),
                "expected1,expected2",
                "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
        });
        /*
         * 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
         */
        data.add(new String[]{
                "Succeeds_HOM_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE a.id=444 AND " +
                        "Interval(2013-06-01T00:11:40, 2013-06-01T00:11:50)" +
                        ".succeeds(e.val)"),
                "expected1",
                "expected1[(s0)-[e0]->(s1)]"
        });
        /*
         * 1.[(Broadway & E14)]
         * 2.[(Hancock St & Bedford Ave)]
         * 3.[(Little West St & 1 Pl)]
         */
        data.add(new String[]{
                "Succeeds_HOM_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a) WHERE " +
                                "Interval(2013-07-10T00:00:01, 2013-07-30)" +
                                ".succeeds(a.tx)"
                ),
                "expected1,expected2,expected3",
                "expected1[(s8)], expected2[(s13)], expected3[(s5)]"

        });
        /*
         * 1.[(Hicks St) -> (Hicks St)]
         */
        data.add(new String[]{
                "Succeeds_HOM_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE e.val_from.precedes(" +
                                "Interval(2013-06-01T00:01:00, 2013-06-01T00:01:01)) " +
                                "AND Interval(2013-07-14, 2013-08-01).succeeds(a.val)"),
                "expected1",
                "expected1[(s2)-[e2]->(s2)]"
        });
        /*
         * (empty)
         */
        data.add(new String[]{
                "Succeeds_HOM_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE e.tx.succeeds(a.tx)"
                ),
                "",
                ""
        });
        /*
         * 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         * 2.[(E 20 St & Park Ave) -> (E 20 St & Park Ave)]
         * 3.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "Succeeds_HOM_8_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE tx.succeeds(Interval(2013-06-01T00:00:00, " +
                                "2013-06-01T00:07:00))"
                ),
                "expected1,expected2,expected3",
                "expected1[(s21)-[e19]->(s11)], expected2[(s27)-[e17]->(s27)], " +
                        "expected3[(s28)-[e18]->(s29)]"
        });

        return data;
    }
}
