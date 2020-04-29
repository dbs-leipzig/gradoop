package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismBetweenData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        /*
         * test to show difference to fromTo, cf. From_ISO_5_default_citibike
         * 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
         * 2.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
         */
        data.add(new String[]{
                "Between_ISO_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE a.id=444 AND e.val.between(2013-06-01T00:00:01,2013-06-01T00:00:08)",
                "expected1,expected2",
                "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
        });
        // same as for fromTo, should work
        /*
         * 1.[(W37 St & 5 Ave) -> (Hicks St)]
         * 2.[(Broadway & E 14) -[edgeId:6]-> (S 5 Pl & S 5 St)]
         * 3.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "FromTo_ISO_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE e.val.between(2013-06-01T00:35:00, 2013-06-01T00:40:00)",
                "expected1,expected2,expected3",
                "expected1[(s7)-[e5]->(s2)]," +
                        "expected2[(s8)-[e6]->(s9)], expected3[(s28)-[e18]->(s29)]"
        });

        /*
         * 1.[(E15 St & Irving Pl) -> (Washington Park)]
         * 2.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         * 3.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "FromTo_ISO_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE NOT e.val.between(2013-06-01T00:04:00, 2013-06-01T00:08:00)",
                "expected1,expected2,expected3",
                "expected1[(s3)-[e3]->(s4)], expected2[(s21)-[e19]->(s11)], " +
                        "expected3[(s28)-[e18]->(s29)]"
        });

        /*
         * 1.[(Broadway & W 29) -[e19]-> (8 Ave & W 31) <-[e13]- (Broadway & W29)]
         * 2.[(Broadway & W 29) -[e13]-> (8 Ave & W 31) <-[e19]- (Broadway & W29)]
         */
        data.add(new String[]{
                "From_ISO_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e1]->(b)<-[e2]-(a) WHERE a.id=486 AND " +
                        "e1.val.between(e2.val_from, e2.val_to)",
                "expected1,expected2",
                "expected1[(s21)-[e19]->(s11)<-[e13]-(s21)]," +
                        "expected2[(s21)-[e13]->(s11)<-[e19]-(s21)]"
        });

        /*
         * 1.[(W37 St & 5 Ave) -> (Hicks St)]
         * 2.[(Stanton St) -> (Hancock St & Bedford Ave)]
         * 3.[(Broadway & E 14) -[edgeId:6]-> (S 5 Pl & S 5 St)]
         * 4.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "From_ISO_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE Interval(2013-06-01T00:34:00,2013-06-01T00:35:00)" +
                        ".between(e.val_from, e.val_to)",
                "expected1,expected2,expected3,expected4",
                "expected1[(s7)-[e5]->(s2)]," +
                        "expected2[(s12)-[e8]->(s13)], expected3[(s8)-[e6]->(s9)]," +
                        "expected4[(s28)-[e18]->(s29)]"
        });

        return data;
    }
}
