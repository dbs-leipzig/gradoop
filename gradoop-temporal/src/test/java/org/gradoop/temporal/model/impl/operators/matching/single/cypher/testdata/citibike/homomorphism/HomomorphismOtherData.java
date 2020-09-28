package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismOtherData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String[]> data = new ArrayList<>();

    // with labels
    // 1. [ (9 Ave & W 18)<-[e0]-(Broadway & W24)-[e1]-> (9 Ave & W  18)]
    // 2. [ (9 Ave & W 18)<-[e1]-(Broadway & W24)-[e0]-> (9 Ave & W  18)]
    data.add(new String[] {
      "Other_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a:station)<-[e1:trip]-(b:station)-[e2:trip]->(c:station) " +
          "WHERE e1.tx_from!=e2.tx_from AND MIN(a.tx_from, b.tx_from, c.tx_from)=Timestamp(2013-05-10)"
      ),
      "expected1,expected2",
      "expected1[(s1)<-[e0]-(s0)-[e1]->(s1)],expected2[(s1)<-[e1]-(s0)-[e0]->(s1)]"
    });

    // path expression and equality constraint on ids
    data.add(new String[] {
      "Other_HOM_2_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a:station)-[e1:trip*1..2]->(b:station)" +
          "WHERE a.id=503 AND a.id=b.id"
      ),
      "expected1,expected2",
      "expected1[(s27)-[e17]->(s27)],expected2[(s27)-[e17]->(s27)-[e17]->(s27)]"
    });


    return data;
  }
}
