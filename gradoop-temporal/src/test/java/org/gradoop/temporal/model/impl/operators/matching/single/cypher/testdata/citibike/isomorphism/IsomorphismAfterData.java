package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Inversions from {@link IsomorphismBeforeData}.
 */
public class IsomorphismAfterData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        /*
         * 1. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:1]-> (9 Ave & W18)]
         * 2. [(E15 St) -> (Washington P.)      (Broadway & W24) -[edgeId:0]-> (9 Ave & W18)]
         */
        data.add(new String[]{
                "Before_ISO_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e1]->(b) (c)-[e2]->(d) WHERE e1.val_from.after(e2.val_from) AND a.id=475",
                "expected1,expected2",
                "expected1[(s3)-[e3]->(s4) (s0)-[e0]->(s1)], " +
                        "expected2[(s3)-[e3]->(s4) (s0)-[e1]->(s1)]"
        });
        /*
         * 1. [(Broadway & E14) -> (S 5 Pl) <- (Henry St & Grand St)]
         */
        data.add(new String[]{
                "Before_ISO_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE b.id=532 AND e1.tx_from.after(e2.tx_from) " +
                        "AND e2.tx_to.after(e1.tx_to)",
                "expected1",
                "expected1[(s8)-[e6]->(s9)<-[e11]-(s18)]"
        });
        /*
         * 1.[(9 Ave & W22) -> (8 Ave & W31) <-[edgeId:19]- (Broadway & W29)]
         * 2.[(9 Ave & W22) -> (8 Ave & W31) <-[edgeId:13]- (Broadway & W29)]
         */
        data.add(new String[]{
                "Before_ISO_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE b.id=521 AND e1.bikeID=16100 " +
                        "AND e2.val_from.after(e1.val_from)",
                "expected1,expected2",
                "expected1[(s10)-[e7]->(s11)<-[e19]-(s21)], " +
                        "expected2[(s10)-[e7]->(s11)<-[e13]-(s21)]"
        });
        /*
         * 1.[Broadway & W24) -[edgeId:0]-> (9 Ave & W18)
         * 2.[Broadway & W24) -[edgeId:1]-> (9 Ave & W18)
         */
        data.add(new String[]{
                "Before_HOM_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                "MATCH (a)-[e]->(b) WHERE 2013-06-01T00:01:00.after(e.tx_from)",
                "expected1,expected2",
                "expected1[(s0)-[e0]->(s1)], expected2[(s0)-[e1]->(s1)]"
        });

        return data;
    }
}
