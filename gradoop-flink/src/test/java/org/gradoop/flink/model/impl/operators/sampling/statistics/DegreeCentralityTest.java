package org.gradoop.flink.model.impl.operators.sampling.statistics;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

public class DegreeCentralityTest extends GradoopFlinkTestBase {

  private String graph =
    "g0[" +
      "(v0)-[]->(v1)"+
      "(v0)-[]->(v2)"+
      "(v0)-[]->(v3)"+
      "(v0)-[]->(v4)"+
      "(v0)-[]->(v5)"+
      "]" +
      "g1[" +
      "(v0)-[]->(v1)"+
      "]";

  @Test
  public void testStar() throws Exception {
  }
}
