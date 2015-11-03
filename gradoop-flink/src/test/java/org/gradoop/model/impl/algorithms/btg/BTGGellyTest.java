package org.gradoop.model.impl.algorithms.btg;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGVertexValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class BTGGellyTest extends FlinkTestBase {
  public BTGGellyTest(TestExecutionMode mode) {
    super(mode);
  }

  private static String[] getConnectedIIG() {
    return new String[] {
      "0,1 0,1 4 9 10", "1,1 1,0 5 6 11 12", "2,1 2,8 13", "3,1 3,7 14 15",
      "4,0 4,0 5", "5,0 5,1 4 6", "6,0 6,1 5 7 8", "7,0 7,3 6", "8,0 8,2 6",
      "9,0 9,0 10", "10,0 10,0 9 11 12", "11,0 11,1 10 13 14",
      "12,0 12,1 10 15", "13,0 13,2 11", "14,0 14,3 11", "15,0 15,3 12"
    };
  }

  static String[] getDisconnectedIIG() {
    return new String[] {
      "0,1 0,6 7", "1,1 1,2 7", "2,1 2,1 8 9", "3,1 3,4 10", "4,1 4,3 5 11 12",
      "5,1 5,4 12 13", "6,0 6,0 7", "7,0 7,0 1 6 8", "8,0 8,2 7 9", "9,0 9,2 8",
      "10,0 10,3 11 12", "11,0 11,4 10", "12,0 12,4 5 10 13", "13,0 13,5"
    };
  }

  @Test
  public void testConnectedIIG() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<Long, BTGVertexValue, NullValue> gellyGraph =
      BTGAlgorithmTestHelper.getGraph(getConnectedIIG(), env);
    DataSet<Vertex<Long, BTGVertexValue>> btgGraph =
      gellyGraph.run(new BTGAlgorithm(maxIteration)).getVertices();
    validateConnectedIIGResult(
      BTGAlgorithmTestHelper.parseResultBTGVertices(btgGraph.collect()));
  }

  @Test
  public void testDisconnectedIIG() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<Long, BTGVertexValue, NullValue> gellyGraph =
      BTGAlgorithmTestHelper.getGraph(getDisconnectedIIG(), env);
    DataSet<Vertex<Long, BTGVertexValue>> btgGraph =
      gellyGraph.run(new BTGAlgorithm(maxIteration)).getVertices();
    validateDisconnectedIIGResult(
      BTGAlgorithmTestHelper.parseResultBTGVertices(btgGraph.collect()));
  }

  private void validateConnectedIIGResult(Map<Long, List<Long>> btgIDs) {
    assertEquals(16, btgIDs.size());
    // master data nodes BTG 1 and 2
    assertEquals(2, btgIDs.get(0L).size());
    assertTrue(btgIDs.get(0L).contains(4L));
    assertTrue(btgIDs.get(0L).contains(9L));
    assertEquals(2, btgIDs.get(1L).size());
    assertTrue(btgIDs.get(1L).contains(4L));
    assertTrue(btgIDs.get(1L).contains(9L));
    assertEquals(2, btgIDs.get(2L).size());
    assertTrue(btgIDs.get(2L).contains(4L));
    assertTrue(btgIDs.get(2L).contains(9L));
    assertEquals(2, btgIDs.get(3L).size());
    assertTrue(btgIDs.get(3L).contains(4L));
    assertTrue(btgIDs.get(3L).contains(9L));
    // transactional data nodes BTG 1
    assertEquals(1, btgIDs.get(4L).size());
    assertTrue(btgIDs.get(4L).contains(4L));
    assertEquals(1, btgIDs.get(5L).size());
    assertTrue(btgIDs.get(5L).contains(4L));
    assertEquals(1, btgIDs.get(6L).size());
    assertTrue(btgIDs.get(6L).contains(4L));
    assertEquals(1, btgIDs.get(7L).size());
    assertTrue(btgIDs.get(7L).contains(4L));
    assertEquals(1, btgIDs.get(8L).size());
    assertTrue(btgIDs.get(8L).contains(4L));
    assertEquals(1, btgIDs.get(9L).size());
    // transactional data nodes BTG 2
    assertTrue(btgIDs.get(9L).contains(9L));
    assertEquals(1, btgIDs.get(10L).size());
    assertTrue(btgIDs.get(10L).contains(9L));
    assertEquals(1, btgIDs.get(11L).size());
    assertTrue(btgIDs.get(11L).contains(9L));
    assertEquals(1, btgIDs.get(12L).size());
    assertTrue(btgIDs.get(12L).contains(9L));
    assertEquals(1, btgIDs.get(13L).size());
    assertTrue(btgIDs.get(13L).contains(9L));
    assertEquals(1, btgIDs.get(14L).size());
    assertTrue(btgIDs.get(14L).contains(9L));
    assertEquals(1, btgIDs.get(15L).size());
    assertTrue(btgIDs.get(15L).contains(9L));
  }

  private void validateDisconnectedIIGResult(Map<Long, List<Long>> btgIDs) {
    assertEquals(14, btgIDs.size());
    // master data nodes BTG 1
    assertEquals(1, btgIDs.get(0L).size());
    assertTrue(btgIDs.get(0L).contains(6L));
    assertEquals(1, btgIDs.get(1L).size());
    assertTrue(btgIDs.get(1L).contains(6L));
    assertEquals(1, btgIDs.get(2L).size());
    assertTrue(btgIDs.get(2L).contains(6L));
    // master data nodes BTG 2
    assertEquals(1, btgIDs.get(3L).size());
    assertTrue(btgIDs.get(3L).contains(10L));
    assertEquals(1, btgIDs.get(4L).size());
    assertTrue(btgIDs.get(4L).contains(10L));
    assertEquals(1, btgIDs.get(5L).size());
    assertTrue(btgIDs.get(5L).contains(10L));
    // transactional data nodes BTG 1
    assertEquals(1, btgIDs.get(6L).size());
    assertTrue(btgIDs.get(6L).contains(6L));
    assertEquals(1, btgIDs.get(7L).size());
    assertTrue(btgIDs.get(7L).contains(6L));
    assertEquals(1, btgIDs.get(8L).size());
    assertTrue(btgIDs.get(8L).contains(6L));
    assertEquals(1, btgIDs.get(9L).size());
    assertTrue(btgIDs.get(9L).contains(6L));
    // transactional data nodes BTG 2
    assertEquals(1, btgIDs.get(10L).size());
    assertTrue(btgIDs.get(10L).contains(10L));
    assertEquals(1, btgIDs.get(11L).size());
    assertTrue(btgIDs.get(11L).contains(10L));
    assertEquals(1, btgIDs.get(12L).size());
    assertTrue(btgIDs.get(12L).contains(10L));
    assertEquals(1, btgIDs.get(13L).size());
    assertTrue(btgIDs.get(13L).contains(10L));
  }
}
