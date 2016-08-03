package org.gradoop.flink.algorithms.fsm;

import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.config.TransactionalFSMAlgorithm;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class TransactionalFSMTest extends GradoopFlinkTestBase {

  @Test
  public void testSingleEdges() throws Exception {

    String asciiGraphs = "" +
      "g1[(v1:A)-[e1:a]->(v2:A)];" +
      "g2[(v1)-[e1]->(v2)];" +
      "g3[(:A)-[:a]->(:A);(:B)-[:b]->(:B);(:B)-[:b]->(:B)]" +
      "g4[(:A)-[:b]->(:A);(:A)-[:b]->(:A);(:A)-[:b]->(:A)];" +
      "s1[(:A)-[:a]->(:A)]";

    String[] searchSpaceVariables = {"g1", "g2", "g3", "g4"};
    String[] expectedResultVariables = {"s1"};

    for(UnaryCollectionToCollectionOperator miner : getDirectedMultigraphMiners()) {

      compareExpectationAndResult(
        miner, asciiGraphs, searchSpaceVariables, expectedResultVariables);
    }
  }

  @Test
  public void testSimpleGraphs() throws Exception {

    String asciiGraphs = "" +
      "g1[(:A)-[:a]->(v1:B)-[:b]->(:C);(v1)-[:c]->(:D)]" +
      "g2[(:A)-[:a]->(v2:B)-[:b]->(:C);(v2)-[:c]->(:E)]" +
      "g3[(:A)-[:a]->(v3:B)-[:d]->(:C);(v3)-[:c]->(:E)]" +
      "s1[(:A)-[:a]->(:B)]" +
      "s2[(:B)-[:b]->(:C)]" +
      "s3[(:B)-[:c]->(:E)]" +
      "s4[(:A)-[:a]->(:B)-[:b]->(:C)]" +
      "s5[(:A)-[:a]->(:B)-[:c]->(:E)]" ;

    String[] searchSpaceVariables = {"g1", "g2", "g3"};
    String[] expectedResultVariables = {"s1", "s2", "s3", "s4", "s5"};

    for(UnaryCollectionToCollectionOperator
      miner : getDirectedMultigraphMiners()) {

      compareExpectationAndResult(
        miner, asciiGraphs, searchSpaceVariables, expectedResultVariables);
    }
  }

  @Test
  public void testParallelEdges() throws Exception {

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(:A)-[:a]->(v1:A)]" +
      "g2[(v2:A)-[:a]->(:A)-[:a]->(v2:A)]" +
      "g3[(:A)-[:a]->(:A)-[:a]->(:A)]" +
      "s1[(:A)-[:a]->(:A)]" +
      "s2[(v3:A)-[:a]->(:A)-[:a]->(v3:A)]";

    String[] searchSpaceVariables = {"g1", "g2", "g3"};
    String[] expectedResultVariables = {"s1", "s2"};


    for(UnaryCollectionToCollectionOperator
      miner : getDirectedMultigraphMiners()) {

      compareExpectationAndResult(
        miner, asciiGraphs, searchSpaceVariables, expectedResultVariables);
    }
  }

  @Test
  public void testLoop() throws Exception {

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v1)-[:a]->(:A)]" +
      "g2[(v2:A)-[:a]->(v2)-[:a]->(:A)]" +
      "g3[(v3:A)-[:a]->(v3)-[:a]->(:A)]" +
      "g4[(:A)-[:a]->(:A)-[:a]->(:A)]" +
      "s1[(:A)-[:a]->(:A)]" +
      "s2[(v3:A)-[:a]->(v3)]" +
      "s3[(v4:A)-[:a]->(v4)-[:a]->(:A)]";

    String[] searchSpaceVariables = {"g1", "g2", "g3", "g4"};
    String[] expectedResultVariables = {"s1", "s2", "s3"};

    for(UnaryCollectionToCollectionOperator
      miner : getDirectedMultigraphMiners()) {

      compareExpectationAndResult(
        miner, asciiGraphs, searchSpaceVariables, expectedResultVariables);
    }
  }

  @Test
  public void testDiamond() throws Exception {

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

    String[] searchSpaceVariables = {"g1", "g2", "g3"};
    String[] expectedResultVariables =
      {"s1", "s2", "s3", "s4", "s5", "s6", "s7"};


    for(UnaryCollectionToCollectionOperator
      miner : getDirectedMultigraphMiners()) {

      compareExpectationAndResult(
        miner, asciiGraphs, searchSpaceVariables, expectedResultVariables);
    }
  }

  @Test
  public void testCircleWithBranch() throws Exception {

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
      "g2[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
      "g3[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +

      "s41[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
      "s31[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)           ]" +
      "s32[(v1:A)-[:a]->(:A)-[:a]->(:A)       (v1)-[:b]->(:B)]" +
      "s33[(v1:A)-[:a]->(:A)       (:A)-[:a]->(v1)-[:b]->(:B)]" +
      "s34[             (:A)-[:a]->(:A)-[:a]->(v1:A)-[:b]->(:B)]" +

      "s21[(:A)-[:a]->(:A)-[:a]->(:A)]" +
      "s22[(:A)-[:a]->(:A)-[:b]->(:B)]" +
      "s23[(:A)<-[:a]-(:A)-[:b]->(:B)]" +

      "s11[(:A)-[:a]->(:A)]" +
      "s12[(:A)-[:b]->(:B)]";

    String[] searchSpaceVariables = {"g1", "g2", "g3"};

    String[] expectedResultVariables =
      {"s11", "s12", "s21", "s22", "s23", "s31", "s32", "s33", "s34", "s41"};

    for(UnaryCollectionToCollectionOperator
      miner : getDirectedMultigraphMiners()) {

      compareExpectationAndResult(
        miner, asciiGraphs, searchSpaceVariables, expectedResultVariables);
    }
  }

  private void compareExpectationAndResult(
    UnaryCollectionToCollectionOperator gSpan, String asciiGraphs,
    String[] searchSpaceVariables, String[] expectedResultVariables) throws
    Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection searchSpace =
      loader.getGraphCollectionByVariables(searchSpaceVariables);

    GraphCollection expectation =
      loader.getGraphCollectionByVariables(expectedResultVariables);

    GraphCollection result =
      gSpan.execute(searchSpace);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }

  private Collection<UnaryCollectionToCollectionOperator
    > getDirectedMultigraphMiners() {

    Collection<UnaryCollectionToCollectionOperator
      > miners = new ArrayList<>();

    float threshold = 0.7f;
    FSMConfig fsmConfig = new FSMConfig(threshold, true);

    miners.add(new TransactionalFSM(
        fsmConfig, TransactionalFSMAlgorithm.GSPAN_BULKITERATION));

    miners.add(new TransactionalFSM(
      fsmConfig, TransactionalFSMAlgorithm.GSPAN_FILTERREFINE));

    return miners;
  }
}