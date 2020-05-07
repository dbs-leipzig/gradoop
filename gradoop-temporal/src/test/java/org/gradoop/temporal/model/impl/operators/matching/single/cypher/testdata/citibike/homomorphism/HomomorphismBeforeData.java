package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;
/**
 * Provides test queries and results to test the before-function (homomorphisms).
 */
public class HomomorphismBeforeData implements TemporalTestData {

    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        /*
         * 1. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
         * 2. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
         * 3. [(E15 St) -> (Washington P.)      (Hicks St) -> (Hicks St)]
         */
        data.add(new String[]{
                "Before_HOM_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE e2.val_from.before(e1.val_from) " +
                        "AND a.id=475"),
                "expected1,expected2,expected3",
                "expected1[(s3)-[e3]->(s4) (s0)-[e0]->(s1)], " +
                        "expected2[(s3)-[e3]->(s4) (s0)-[e1]->(s1)], " +
                        "expected3[(s3)-[e3]->(s4) (s2)-[e2]->(s2)]"
        });

        /*
         * 1. [(Broadway & W29)-[edgeId:13]->(8 Ave & W31)      (Broadway & W29)-[edgeId:19]->(8 Ave & W31)]
         */
        data.add(new String[]{
                "Before_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE a.id=486 " +
                        "AND c.id=486 AND e1.val_to.before(e2.val_to)"),
                "expected1",
                "expected1[(s21)-[e13]->(s11) (s21)-[e19]->(s11)]"
        });

        /*
         * 1. [(Hicks St & Montague) -> (Hicks St & Montague) <- (W 37 St & 5 Ave)]
         */
        data.add(new String[]{
                "Before_HOM_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE b.id=406 AND e1.tx_from.before(e2.tx_from)"),
                "expected1",
                "expected1[(s2)-[e2]->(s2)<-[e5]-(s7)]"
        });

        /*
         * 1.[(Hicks St)->(Hicks St)]
         * 2.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
         * 3.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
         */
        data.add(new String[]{
                "Before_HOM_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE e.tx_from.before(2013-06-01T00:01:00)"),
                "expected1,expected2,expected3",
                "expected1[(s2)-[e2]->(s2)], expected2[(s0)-[e0]->(s1)], expected3[(s0)-[e1]->(s1)]"
        });
        /*
         * 1.[(Broadway & E14)]
         * 2.[(Hancock St & Bedford Ave)]
         * 3.[(Little West St & 1 Pl)]
         */
        data.add(new String[]{
                "Before_HOM_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a) WHERE a.tx_to.before(2013-07-11)"
                ),
                "expected1,expected2,expected3",
                "expected1[(s8)], expected2[(s13)], expected3[(s5)]"

        });
        /*
         * 1.[(Hicks St)->(Hicks St)]
         */
        data.add(new String[]{
                "Before_HOM_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE e.tx_from.before(2013-06-01T00:01:00)" +
                                " AND 2013-05-12.before(a.val_from)" +
                                "AND a.val_from.before(e.tx_from)"),
                "expected1",
                "expected1[(s2)-[e2]->(s2)]"
        });
        /*
         * (empty)
         */
        data.add(new String[]{
                "Before_HOM_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE e.val_from.before(a.val_from)"
                ),
                "",
                ""
        });
        /*
         * 1.[(Broadway & E14)]
         */
        data.add(new String[]{
                "Before_HOM_8_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a) WHERE a.tx_to.before(2013-07-11) AND" +
                                " 2013-05-28.before(val_from)"
                ),
                "expected1",
                "expected1[(s8)]"

        });
        return data;
    }
}
