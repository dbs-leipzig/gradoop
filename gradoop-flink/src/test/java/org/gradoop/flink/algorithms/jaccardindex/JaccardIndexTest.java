package org.gradoop.flink.algorithms.jaccardindex;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator.MAX;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator.UNION;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.IN;
import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.OUT;

public class JaccardIndexTest extends GradoopFlinkTestBase {

  /**
   * Tests a minimal directed graph with outgoing neighborhood configuration
   * @throws Exception
   */
  @Test
  public void testMinimalGraphOut() throws Exception {
    String graph =
      "(v0:A)-[:e]->(v1:B) " +
      "(v2:C)-[:e]->(v1)";
    String res =
      "(v0)-[:ji {value: 1.0d}]->(v2) " +
      "(v2)-[:ji {value: 1.0d}]->(v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex("ji", OUT, UNION);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  /**
   * Tests a directed graph with outgoing neighborhood configuration
   * @throws Exception
   */
  @Test
  public void testOut() throws Exception {
    String graph =
      "(v0:A)-[:e]->(v1:B) " +
      "(v0)-[:e]->(v2:C) " +
      "(v1)-[:e]->(v2)";
    String res =
      "(v0)-[:jaccardSimilarity {value: .5d}]->(v1) " +
      "(v1)-[:jaccardSimilarity {value: .5d}]->(v0)";

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
    String graph =
      "(v0:A)<-[:e]-(v1:B) " +
      "(v2:C)<-[:e]-(v1)";
    String res =
      "(v0)-[:ji {value: 1.0d}]->(v2) " +
      "(v2)-[:ji {value: 1.0d}]->(v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex("ji", IN, UNION);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  /**
   * Tests a directed graph with incoming neighborhood configuration
   * @throws Exception
   */
  @Test
  public void testIn() throws Exception {
    String graph =
      "(v0:A)-[:e]->(v1:B) " +
      "(v0)-[:e]->(v2:C) " +
      "(v1)-[:e]->(v2)";
    String res =
      "(v1)-[:jaccardSimilarity {value: .5d}]->(v2) " +
      "(v2)-[:jaccardSimilarity {value: .5d}]->(v1)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResult =
      getLoaderFromString("input[" + graph + res + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndex = new JaccardIndex("jaccardSimilarity", IN, UNION);

    LogicalGraph result = input.callForGraph(jaccardIndex);
    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }

  /**
   * Tests the maximum denominator in combination with outgoing neighborhood
   */
  @Test
  public void testMax() throws Exception{
    String graph =
      "(v0:A)-[:e]->(v1:B) " +
      "(v0)-[:e]->(v2:C) " +
      "(v1)-[:e]->(v2) " +
      "(v0)-[:e]->(v3:D) " +
      "(v0)-[:e]->(v4:E)" +
      "(v1)-[:e]->(v5:F)";

    String resMax =
      "(v0)-[:jaccardSimilarity {value: .25d}]->(v1) " +
      "(v1)-[:jaccardSimilarity {value: .25d}]->(v0)";
    String resUnion =
      "(v0)-[:jaccardSimilarity {value: .2d}]->(v1) " +
      "(v1)-[:jaccardSimilarity {value: .2d}]->(v0)";

    LogicalGraph input =
      getLoaderFromString("input[" + graph + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResultMax =
      getLoaderFromString("input[" + graph + resMax + "]").getLogicalGraphByVariable("input");
    LogicalGraph expectedResultUnion =
      getLoaderFromString("input[" + graph + resUnion + "]").getLogicalGraphByVariable("input");

    JaccardIndex jaccardIndexMax = new JaccardIndex("jaccardSimilarity", OUT, MAX);
    JaccardIndex jaccardIndexUnion = new JaccardIndex("jaccardSimilarity", OUT, UNION);

    LogicalGraph resultMax = input.callForGraph(jaccardIndexMax);
    LogicalGraph resultUnion = input.callForGraph(jaccardIndexUnion);

    collectAndAssertTrue(resultMax.equalsByElementData(expectedResultMax));
    collectAndAssertTrue(resultUnion.equalsByElementData(expectedResultUnion));
  }


}
