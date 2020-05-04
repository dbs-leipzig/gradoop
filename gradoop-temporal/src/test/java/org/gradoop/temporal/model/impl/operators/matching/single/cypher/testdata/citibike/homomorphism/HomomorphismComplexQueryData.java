package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismComplexQueryData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        /*
         * 1.[(Hicks St)->(Hicks St)]
         */
        data.add(new String[]{
                "Complex_HOM_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE 2013-06-01T00:01:00.after(e.tx_from) " +
                                "AND NOT b.tx_from.after(a.tx_from) AND e.asOf(2013-06-01T00:01:00)"),
                "expected1",
                "expected1[(s2)-[e2]->(s2)]"
        });

        /*
         * 1.[(Hicks St) -> (Hicks St)]
         * 2.[(Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
         * 3.[(Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
         * 4.[(W37 St & 5 Ave) -> (Hicks St)]
         * 5.[(Broadway & E 14) -[edgeId:6]-> (S 5 Pl & S 5 St)]
         * 6.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "Complex_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE e.asOf(2013-06-01T00:01:00) OR  val.fromTo(2013-06-01T00:35:00," +
                        " 2013-06-01T00:40:00)",
                "expected1,expected2,expected3,expected4,expected5,expected6",
                "expected1[(s2)-[e2]->(s2)], expected2[(s0)-[e0]->(s1)]," +
                        "expected3[(s0)-[e1]->(s1)], expected4[(s7)-[e5]->(s2)], " +
                        " expected5[(s8)-[e6]->(s9)], expected6[(s28)-[e18]->(s29)]"
        });

        return data;
    }
}
