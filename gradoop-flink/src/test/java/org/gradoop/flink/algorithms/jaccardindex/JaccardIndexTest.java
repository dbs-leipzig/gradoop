package org.gradoop.flink.algorithms.jaccardindex;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Direction.INDEGREE;

public class JaccardIndexTest extends GradoopFlinkTestBase {

  @Test
  public void testMinimalDirectedGraphOutDegree() throws Exception {
    String graph = "" + "(v0:A)-[:e]->(v1:B)  " + "(v2:C)-[:e]->(v1)";
    String res = "(v0) -[:ji {value : 1.0d}]-> (v2)" + "(v2) -[:ji {value : 1.0d}]-> (v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex();
    jaccardIndex.setEdgeLabel("ji");

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  @Test
  public void testDirectedGraphOutDegree() throws Exception {
    String graph = "" + "(v0:A)-[:e]->(v1:B)  " + "(v0)-[:e]->(v2:C)" + "(v1)-[:e]->(v2)";
    String res = "(v0) -[:jaccardSimilarity {value : .5d}]-> (v1)" +
      "(v1) -[:jaccardSimilarity {value : .5d}]-> (v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex();

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  @Test
  public void testMinimalDirectedGraphInDegree() throws Exception {
    String graph = "" + "(v0:A)<-[:e]-(v1:B)  " + "(v2:C)<-[:e]-(v1)";
    String res = "(v0) -[:ji {value : 1.0d}]-> (v2)" + "(v2) -[:ji {value : 1.0d}]-> (v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex();
    jaccardIndex.setEdgeLabel("ji");
    jaccardIndex.setDirection(JaccardIndex.Direction.INDEGREE);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  @Test
  public void testDirectedGraphInDegree() throws Exception {
    String graph = "" + "(v0:A)-[:e]->(v1:B)  " + "(v0)-[:e]->(v2:C)" + "(v1)-[:e]->(v2)";
    String res = "(v1) -[:jaccardSimilarity {value : .5d}]-> (v2)" +
      "(v2) -[:jaccardSimilarity {value : .5d}]-> (v1)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex();
    jaccardIndex.setDirection(INDEGREE);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

}
