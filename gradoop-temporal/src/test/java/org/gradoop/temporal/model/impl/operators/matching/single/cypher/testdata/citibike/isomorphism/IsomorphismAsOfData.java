package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismAsOfData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        /*
         * 1.[(Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
         * 2.[(Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
         */
        data.add(new String[]{
                "Before_HOM_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE e.asOf(2013-06-01T00:01:00)",
                "expected1,expected2",
                "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
        });
        /*
         * 1.[(Greenwich St & W Houston)<-(Murray St & West St)->(Shevchenko Pl)]
         */
        data.add(new String[]{
                "AsOf_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE b.id=309 AND e2.asOf(e1.val_from)",
                "expected1",
                "expected1[(s26)<-[e16]-(s24)-[e15]->(s25)]"
        });

        /*
         * 1.[(Broadway & W 29 St) -[edgeId:19]-> (8 Ave & W 31)]
         * 2.[(E15 St & Irving) -> (Washington Park)
         * 2.[(Lispenard St) -> (Broadway & W 51)]
         */
        data.add(new String[]{
                "AsOf_ISO3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE NOT e.asOf(2013-06-01T00:08:00)",
                "expected1,expected2,expected3",
                "expected1[(s21)-[e19]->(s11)], expected2[(s3)-[e3]->(s4)]," +
                        " expected3[(s28)-[e18]->(s29)]"
        });

        /*
         * 1.[(Lispenard St) -> (Broadway & W 51)]
         */
        data.add(new String[]{
                "AsOf_ISO4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE NOT e.asOf(2013-06-01T00:08:00) AND " +
                        "b.asOf(2013-05-20)",
                "expected1",
                "expected1[(s28)-[e18]->(s29)]"
        });
        return data;
    }
}