package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.isomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

/**
 * almost the same as for homomorphism
 */
public class IsomorphismEqualsTest implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        //empty
        data.add(new String[]{
                "Equals_ISO1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.tx.equals(b.tx)"
                ),
                "",
                ""
        });

        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_ISO_2_default_citibike",
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
                "Equals_ISO_3_default_citibike",
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
                "Equals_ISO_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.tx.equals(e.tx) OR e.tx.equals(b.tx)"
                ),
                "",
                ""
        });

        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_ISO_5_default_citibike",
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
                "Equals_ISO_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE a.val.join(b.val).equals(Interval(" +
                                "2013-05-15, 2013-07-23))"
                ),
                "expected1",
                "expected1[(s14)-[e9]->(s15)]"
        });

        //empty
        data.add(new String[]{
                "Equals_ISO_7_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH(a)-[e1]->(b)<-[e2]-(c) WHERE a.tx.merge(b.tx).equals(c.tx.merge(b.tx))"
                ),
                "",
                ""
        });

        // 1.[(9 Ave & W14 St)->(Mercer St & Spring St)]
        data.add(new String[]{
                "Equals_ISO_8_default_citibike",
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
                "Equals_ISO_9_default_citibike",
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
