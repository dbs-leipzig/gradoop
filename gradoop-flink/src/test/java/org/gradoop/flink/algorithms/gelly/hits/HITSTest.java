package org.gradoop.flink.algorithms.gelly.hits;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

public class HITSTest extends GradoopFlinkTestBase {

  @Test
  public void minimalHITSTest() throws Exception {

    String inputString = "input[(v0:A)-[:e]->(v1:B)<-[:e]-(v2:C)]";

    // Values are normalized sqrt(0.5) = .7071067811865475d
    String expectedResultString = "input[" +
      "(v0:A {aScore: 0.0d, hScore: .7071067811865475d})-[:e]->" +
      "(v1:B {aScore: 1.0d, hScore: 0.0d})<-[:e]-" +
      "(v2:C {aScore: 0.0d, hScore: .7071067811865475d})]";

    LogicalGraph input = getLoaderFromString(inputString)
      .getLogicalGraphByVariable("input");
    LogicalGraph expectedResult = getLoaderFromString(expectedResultString)
      .getLogicalGraphByVariable("input");
    LogicalGraph result = input.callForGraph(new HITS("aScore", "hScore", 1));

    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }
}
