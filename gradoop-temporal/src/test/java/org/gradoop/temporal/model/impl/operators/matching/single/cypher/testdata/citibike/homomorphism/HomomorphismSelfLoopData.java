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
      "ImmPrecedes_HOM_6_default_citibike",
      CBCypherTemporalPatternMatchingTest.defaultData,
      CBCypherTemporalPatternMatchingTest.noDefaultAsOf(
        "MATCH (a)-[e]->(a)"
      ),
      "",""
    });
    return data;
  }
}
