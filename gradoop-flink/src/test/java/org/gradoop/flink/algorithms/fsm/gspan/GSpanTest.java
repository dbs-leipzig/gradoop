package org.gradoop.flink.algorithms.fsm.gspan;

import com.google.common.collect.Lists;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.GSpanGraphCollectionEncoder;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DirectedDFSStep;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

public class GSpanTest extends GradoopFlinkTestBase {

  @Test
  public void testMinDfsCodeCalculation() {

    float threshold = 0.7f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    //       -a->
    //  (0:A)    (1:A)
    //       -a->

    DFSStep firstStep = new DirectedDFSStep(0, 0, true, 0, 1, 0);
    DFSStep backwardStep = new DirectedDFSStep(1, 0, false, 0, 0, 0);
    DFSStep branchStep = new DirectedDFSStep(0, 0, true, 0, 1, 0);

    DFSCode minCode = new DFSCode(Lists.newArrayList(firstStep, backwardStep));
    DFSCode wrongCode = new DFSCode(Lists.newArrayList(firstStep, branchStep));

    assertTrue(
      GSpan.isMinimal(minCode, fsmConfig));
    assertFalse(
      GSpan.isMinimal(wrongCode, fsmConfig));
  }

  @Test
  public void testDiamondMining() throws Exception {

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
      "g2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
      "g3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

      "s1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

      "s2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)             ]" +
      "s3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);             (v3:A)-[:a]->(v4:A)]" +
      "s4[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A)                                 ]" +
      "s5[(v1:A)-[:a]->(v2:A)             ;(v1:A)-[:a]->(v3:A)             ]" +
      "s6[             (v2:A)-[:a]->(v4:A);             (v3:A)-[:a]->(v4:A)]" +
      "s7[(v1:A)-[:a]->(v2:A)                                              ]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    GraphCollection searchSpace = loader.getGraphCollectionByVariables("g1");

    float threshold = 0.7f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    GSpanEncoder<GraphCollection> encoder = new GSpanGraphCollectionEncoder(
      fsmConfig);

    Collection<GSpanGraph> graphs =
      encoder.encode(searchSpace, fsmConfig).collect();

    // create GSpanGraph
    GSpanGraph transaction = graphs.iterator().next();

    assertEquals(1, transaction.getSubgraphEmbeddings().size());

    assertEquals(4,
      transaction.getSubgraphEmbeddings().values().iterator().next().size());

    // N=1
    Collection<DFSCode> singleEdgeCodes =
      transaction.getSubgraphEmbeddings().keySet();

    assertEquals(singleEdgeCodes.size(), 1);

    DFSCode singleEdgeCode =
      singleEdgeCodes.iterator().next();

    assertEquals(singleEdgeCode, new DFSCode(new DirectedDFSStep(0, 0, true, 0, 1, 0)));

    // N=2
    assertEquals(0, singleEdgeCode.getMinVertexLabel());

    GSpan.growEmbeddings(transaction, singleEdgeCodes,fsmConfig);

    Collection<DFSCode> twoEdgeCodes =
      transaction.getSubgraphEmbeddings().keySet();

    assertEquals(4, twoEdgeCodes.size());

    // post pruning
    Iterator<DFSCode> iterator = twoEdgeCodes.iterator();

    while (iterator.hasNext()) {
      DFSCode subgraph = iterator.next();

      if (!GSpan.isMinimal(subgraph, fsmConfig)) {
        iterator.remove();
      }
    }

    assertEquals(3, twoEdgeCodes.size());

    // N=3

    DFSCode minSubgraph = GSpan.selectMinDFSCode(twoEdgeCodes, fsmConfig);

    GSpan.growEmbeddings(
      transaction, Lists.newArrayList(minSubgraph), fsmConfig);

    Collection<DFSCode> threeEdgeCodes =
      transaction.getSubgraphEmbeddings().keySet();

    assertEquals(2, threeEdgeCodes.size());

    // post pruning
    iterator = threeEdgeCodes.iterator();

    while (iterator.hasNext()) {
      DFSCode subgraph = iterator.next();

      if (!GSpan.isMinimal(subgraph, fsmConfig)) {
        iterator.remove();
      }
    }

    assertEquals(2, threeEdgeCodes.size());

    // N=4

    minSubgraph = GSpan.selectMinDFSCode(threeEdgeCodes, fsmConfig);

    GSpan.growEmbeddings(
      transaction, Lists.newArrayList(minSubgraph), fsmConfig);

    Collection<DFSCode> fourEdgeCodes =
      transaction.getSubgraphEmbeddings().keySet();

    assertEquals(1, fourEdgeCodes.size());
  }
}