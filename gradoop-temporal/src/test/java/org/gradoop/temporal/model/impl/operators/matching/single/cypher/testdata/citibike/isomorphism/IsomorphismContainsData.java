package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class IsomorphismContainsData implements TemporalTestData {


    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        //1.[(Broadway & E14) -> (S 5 Pl & S 5 St) <- (Henry St & Grand St)]
        data.add(new String[]{
                "Contains_HOM_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE e1!=e2 AND e1.val.contains(e2.val)"
                ),
                "expected1",
                "expected1[(s8)-[e6]->(s9)<-[e11]-(s18)]"
        });
        //1.[(Broadway & E14)->(S5 Pl & S 5 St)]
        //2.[(W37 St & 5 Ave)->(Hicks St & Montague St)]
        data.add(new String[]{
                "Contains_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE e.val.contains(2013-06-01T00:35:35) AND " +
                                "NOT b.tx.contains(2013-07-17)"
                ),
                "expected1,expected2",
                "expected1[(s8)-[e6]->(s9)], expected2[(s7)-[e5]->(s2)]"
        });

        // 1.[(Murray St & West St) -> (Shevchenko Pl)]
        data.add(new String[]{
                "Contains_HOM_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).contains(Interval(" +
                                "2013-05-12,2013-07-28))"
                ),
                "expected1",
                "expected1[(s24)-[e15]->(s25)]"
        });

        //(empty)
        data.add(new String[]{
                "Contains_HOM_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE NOT(a.tx.contains(b.tx_from) OR b.val.contains(b.tx_to))"
                ),
                "",
                ""
        });

        return data;
    }
}
