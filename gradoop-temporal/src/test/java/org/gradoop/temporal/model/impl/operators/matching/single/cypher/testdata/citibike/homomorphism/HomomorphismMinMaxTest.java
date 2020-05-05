package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismMinMaxTest implements TemporalTestData {

    @Override
    public Collection<String[]> getData() {
        ArrayList<String[]> data = new ArrayList<>();
        // 1. [ (9 Ave & W 18)<-[e0]-(Broadway & W24)-[e1]-> (9 Ave & W  18)]
        // 2. [ (9 Ave & W 18)<-[e1]-(Broadway & W24)-[e0]-> (9 Ave & W  18)]
        data.add(new String[]{
                "MinMax_HOM_1_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)<-[e1]-(b)-[e2]->(c) " +
                                "WHERE e1.tx_from!=e2.tx_from AND MIN(a.tx_from, b.tx_from, c.tx_from)=2013-05-10"
                ),
                "expected1,expected2",
                "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s1)<-[e1]-(s0)-[e0]->(s1)]"
        });
        // 1. [ (9 Ave & W 18)<-[e0]-(Broadway & W24)-[e1]-> (9 Ave & W  18)]
        // 2. [ (9 Ave & W 18)<-[e1]-(Broadway & W24)-[e0]-> (9 Ave & W  18)]
        data.add(new String[]{
                "MinMax_HOM_2_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)<-[e1]-(b)-[e2]->(c) " +
                                "WHERE e1.tx_from!=e2.tx_from AND MAX(a.tx_to, b.tx_to, c.tx_to)=2013-07-18"
                ),
                "expected1,expected2",
                "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s1)<-[e1]-(s0)-[e0]->(s1)]"
        });

        // empty
        data.add(new String[]{
                "MinMax_HOM_3_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE val_from!=MAX(a.val_from,e.val_from) OR " +
                                "val_to!=MIN(b.val_to,e.val_to)"
                ),
                "",
                ""
        });

        // empty
        data.add(new String[]{
                "MinMax_HOM_4_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE NOT a.tx.join(b.tx).equals(" +
                                "Interval(MIN(a.tx_from, b.tx_from), MAX(a.tx_to, b.tx_to)))"
                ),
                "",
                ""
        });

        // 1.[(Broadway & E14) -> (S 5 Pl)]
        // 2.[(E20 St & Park Ave) -> (E20 St & Park Ave)]
        // 3.[(E15 St) -> (Washington Park)]
        data.add(new String[]{
                "MinMax_HOM_5_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)-[e]->(b) WHERE MIN(a.tx_from, b.tx_from, e.tx_from)>=2013-05-26"
                ),
                "expected1,expected2,expected3",
                "expected1[(s8)-[e6]->(s9)],expected2[(s27)-[e17]->(s27)],expected3[(s3)-[e3]->(s4)]"
        });

        // 1. [(9 Ave & W 18 St) <-[e0]- (Broadway & 24 St) -[e1]-> (9 Ave & W18 St) ]
        // 2. [(Shevchenko Pl) <- (Murray St & West St) -> (Greenwich St & Houston St) ]
        // 3. [(8 Ave & W31) <-[e13]- (Broadway & W 29) -[e19]-> (8 Ave & W31) ]
        data.add(new String[]{
                "MinMax_HOM_6_default_citibike",
                CBCypherTemporalPatternMatchingTest.defaultData,
                CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
                        "MATCH (a)<-[e1]-(b)-[e2]->(c) WHERE e1.val_from!=e2.val_from AND " +
                                "e1.val_from = MIN(e1.val_from, e2.val_from, a.tx_to, b.tx_to, c.tx_to)"
                ),
                "expected1,expected2,expected3",
                "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s25)<-[e15]-(s24)-[e16]->(s26)]," +
                        "expected3[(s11)<-[e13]-(s21)-[e19]->(s11)],"
        });
        return data;
    }
}