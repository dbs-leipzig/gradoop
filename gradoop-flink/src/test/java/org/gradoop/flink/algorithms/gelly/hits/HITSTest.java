package org.gradoop.flink.algorithms.gelly.hits;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

public class HITSTest extends GradoopFlinkTestBase {

  @Test
  public void minimalHITSTest() throws Exception {

    String inputString = "input[" + "(v0:v0) -[:e]-> (v2:v2) " + "(v1:v1) -[:e]-> (v2) " + "]";

    // Values are normalized sqrt(0.5) = .7071067811865475d
    String expectedResultString = "input[" +
      "(v0:v0 {aScore : 0.0d, hScore : .7071067811865475d}) -[:e]-> (v2:v2 {aScore : 1.0d, hScore" +
      " : 0.0d})" +
      "(v1:v1 {aScore : 0.0d, hScore : .7071067811865475d}) -[:e]-> (v2) " + "]";

    LogicalGraph input = getLoaderFromString(inputString).getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString(expectedResultString).getLogicalGraphByVariable("input");
    LogicalGraph result = input.callForGraph(new HITS("aScore", "hScore", 1));

    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }
}
