package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismSucceedsData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        
         // 1.[(E 15 St & Irving)->(Washington Park)  (Henry St & Grand St)->(S5 Pl & S 5 St)]
        data.add(new String[]{
                "Succeeds_ISO_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH ()-[e1]->() ()-[e2]->(a) WHERE a.id=532 AND e1.edgeId=3" +
                        " AND e2.val.succeeds(e1.val)"),
                "expected1",
                "expected1[(s3)-[e3]->(s4) (s18)-[e11]->(s9)]"
        });
        
         // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         // 2.[(Lispenard St) -> (Broadway & W 51 St)]
        data.add(new String[]{
                "Succeeds_ISO_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE e.tx" +
                        ".succeeds(Interval(2013-06-01T00:00:00, 2013-06-01T00:07:00))"),
                "expected1,expected2",
                "expected1[(s21)-[e19]->(s11)]," +
                        "expected2[(s28)-[e18]->(s29)]"
        });
        
         // same as above, but now testing call from timestamp
         // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
         // 2.[(Lispenard St) -> (Broadway & W 51 St)]
        data.add(new String[]{
                "Succeeds_ISO_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE e.tx_from.succeeds(" +
                        "Interval(2013-06-01T00:06:00,2013-06-01T00:07:00))"),
                "expected1,expected2",
                "expected1[(s21)-[e19]->(s11)]," +
                        "expected2[(s28)-[e18]->(s29)]"
        });
        
         // 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
         // 2.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
        data.add(new String[]{
                "Succeeds_ISO_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE a.id=444 AND " +
                        "Interval(2013-06-01T00:11:41, 2013-06-01T00:11:50)" +
                        ".succeeds(e.val)"),
                "expected1,expected2",
                "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
        });
        
         // 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
        data.add(new String[]{
                "Succeeds_ISO_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                "MATCH (a)-[e]->(b) WHERE a.id=444 AND " +
                        "Interval(2013-06-01T00:11:40, 2013-06-01T00:11:50)" +
                        ".succeeds(e.val)"),
                "expected1",
                "expected1[(s0)-[e0]->(s1)]"
        });
        
         // 1.[(Lispenard St) -> (Broadway & W 51 St)]
        data.add(new String[]{
                "Succeeds_ISO_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE 2013-06-01T00:07:00.precedes(e.tx)" +
                                " AND 2013-05-12.succeeds(" +
                                "Interval(1970-01-01,a.val_from))"),
                "expected1",
                "expected1[(s28)-[e18]->(s29)]"
        });

        // 1.[(Broadway & W29) -[edgeId:19]->(8 Ave & W31)]
        // 2.[(Lispenard St) -> (Broadway & W 51 St)]
        data.add(new String[]{
                "Succeeds_ISO_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE MAX(a.val_from, e.tx_from)" +
                                ".succeeds(Interval(2013-06-01T00:00:00," +
                                "2013-06-01T00:07:00))"),
                "expected1,expected2",
                "expected1[(s21)-[e19]->(s11)], " +
                        "expected2[(s28)-[e18]->(s29)]"
        });

        return data;
    }
}
