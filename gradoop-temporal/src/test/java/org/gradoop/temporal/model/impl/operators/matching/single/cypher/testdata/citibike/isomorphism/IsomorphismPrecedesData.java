package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismPrecedesData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();

        /*
         * 1.[(E 15 St & Irving)->(Washington Park)  (Henry St & Grand St)->(S5 Pl & S 5 St)]
         */
        data.add(new String[]{
                "Precedes_ISO_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH ()-[e1]->() ()-[e2]->(a) WHERE a.id=532 AND e1.edgeId=3" +
                        " AND e1.val.precedes(e2.val)",
                "expected1",
                "expected1[(s3)-[e3]->(s4) (s18)-[e11]->(s9)]"
        });
        /*
         * 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         * 2.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "Precedes_ISO_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE Interval(2013-06-01T00:00:00, 2013-06-01T00:07:00)" +
                        ".precedes(e.tx)",
                "expected1,expected2",
                "expected1[(s21)-[e19]->(s11)]," +
                        "expected2[(s28)-[e18]->(s29)]"
        });
        /*
         * same as above, but now testing call from timestamp
         * 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         * 2.[(Lispenard St) -> (Broadway & W 51 St)]
         */
        data.add(new String[]{
                "Precedes_ISO_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE 2013-06-01T00:07:00.precedes(e.tx)",
                "expected1,expected2",
                "expected1[(s21)-[e19]->(s11)]," +
                        "expected2[(s28)-[e18]->(s29)]"
        });
        /*
         * 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
         * 2.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
         */
        data.add(new String[]{
                "Precedes_ISO_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE e.val_from.precedes(" +
                        "Interval(2013-06-01T00:01:00, 2013-06-01T00:01:01))",
                "expected1,expected2",
                "expected1[(s0)-[e0]->(s1)], " +
                        "expected2[(s0)-[e1]->(s1)]"
        });

        return data;
    }
}
