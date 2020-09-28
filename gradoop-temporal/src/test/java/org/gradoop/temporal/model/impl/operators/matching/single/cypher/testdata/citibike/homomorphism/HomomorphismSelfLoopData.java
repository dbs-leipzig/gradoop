package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.util.ArrayList;
import java.util.Collection;

public class HomomorphismSelfLoopData implements TemporalTestData {
  @Override
  public Collection<String[]> getData() {
    ArrayList<String []> data = new ArrayList<>();
    data.add(new String[]{
      "SelfLoop_HOM_1_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.prepareQueryString(
        "MATCH (a)-[e]->(a)"
      ),
      "expected1,expected2","expected1[(s2)-[e2]->(s2)],expected2[(s27)-[e17]->(s27)]"
    });
    return data;
  }
}
