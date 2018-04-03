package org.gradoop.flink.algorithms.jaccardindex;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator.MAX;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.IN;

public class JaccardIndexTest extends GradoopFlinkTestBase {

  /**
   * Tests a minimal directed graph with outgoing neighborhood configuration
   * @throws Exception
   */
  @Test
  public void testMinimalGraphOut() throws Exception {
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

  /**
   *  Tests a directed graph with outgoing neighborhood configuration
   * @throws Exception
   */
  @Test
  public void testOut() throws Exception {
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

  /**
   * Tests a minimal directed graph with incoming neighborhood configuration
   * @throws Exception
   */
  @Test
  public void testMinimalGraphIn() throws Exception {
    String graph = "" + "(v0:A)<-[:e]-(v1:B)  " + "(v2:C)<-[:e]-(v1)";
    String res = "(v0) -[:ji {value : 1.0d}]-> (v2)" + "(v2) -[:ji {value : 1.0d}]-> (v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex();
    jaccardIndex.setEdgeLabel("ji");
    jaccardIndex.setNeighborhoodType(JaccardIndex.NeighborhoodType.IN);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  /**
   * Tests a directed graph with incoming neighborhood configuration
   * @throws Exception
   */
  @Test
  public void testIn() throws Exception {
    String graph = "" + "(v0:A)-[:e]->(v1:B)  " + "(v0)-[:e]->(v2:C)" + "(v1)-[:e]->(v2)";
    String res = "(v1) -[:jaccardSimilarity {value : .5d}]-> (v2)" +
      "(v2) -[:jaccardSimilarity {value : .5d}]-> (v1)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex();
    jaccardIndex.setNeighborhoodType(IN);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  /**
   * Tests the maximum denominator in combination with outgoing neighborhood
   */
  @Test
  public void testOutMax() throws Exception{
    String graph = ""+
      "(v0:A)-[:e]->(v1:B) " +
      "(v0)-[:e]->(v2:C) " +
      "(v1)-[:e]->(v2) " +
      "(v0)-[:e]->(v3:D) " +
      "(v0)-[:e]->(v4:C)";

    String res = "(v0) -[:jaccardSimilarity {value : .25d}]-> (v1)" +
      "(v1) -[:jaccardSimilarity {value : .25d}]-> (v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex();
    jaccardIndex.setDenominator(MAX);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }


}
