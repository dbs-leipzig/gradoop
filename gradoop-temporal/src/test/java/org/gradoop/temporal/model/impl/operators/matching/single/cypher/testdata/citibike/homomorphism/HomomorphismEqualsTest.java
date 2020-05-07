package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismEqualsTest implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        //1.[(Hicks St)->(Hicks St)]
        //2.[(E20 St & Park Ave) -> (E20 St & Park Ave)]
        data.add(new String[]{
                "Equals_HOM1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.tx.equals(b.tx)"
                ),
                "expected1,expected2",
                "expected1[(s2)-[e2]->(s2)], expected2[(s27)-[e17]->(s27)]"
        });
        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE e.val.equals(Interval(2013-06-01T00:04:22," +
                                " 2013-06-01T00:18:11))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });
        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_HOM_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.equals(Interval(2013-05-15," +
                                " 2013-07-23)) AND Interval(2013-05-20,2013-07-18).equals(b.val)"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });
        // empty
        data.add(new String[]{
                "Equals_HOM_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.tx.equals(e.tx) OR e.tx.equals(b.tx)"
                ),
                "",
                ""
        });
        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_HOM_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.merge(b.val).equals(Interval(" +
                                "2013-05-20, 2013-07-18))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_HOM_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).equals(Interval(" +
                                "2013-05-15, 2013-07-23))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_HOM_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE val.equals(Interval(" +
                                "2013-06-01T00:04:22, 2013-06-01T00:18:11))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        // empty
        data.add(new String[]{
                "Equals_HOM_8_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e1]->(b)<-[e2]-(c) WHERE NOT tx.equals(e1.tx.merge(e2.tx))"
                ),
                "",
                ""
        });

        return data;
    }
}
