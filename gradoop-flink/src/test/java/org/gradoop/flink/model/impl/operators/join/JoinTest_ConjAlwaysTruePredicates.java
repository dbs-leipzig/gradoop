package org.gradoop.flink.model.impl.operators.join;

import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.edgesemantics.PredefinedEdgeSemantics;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by vasistas on 01/02/17.
 */
public class JoinTest_ConjAlwaysTruePredicates extends GradoopFlinkTestBase {

  private static JoinType GRAPH_INNER_JOIN = JoinType.INNER;
  private static final  PredefinedEdgeSemantics CONJUNCTIVE_BASICS = PredefinedEdgeSemantics
    .CONJUNCTIVE;
  private static final Function<Tuple2<String,String>,String> BASIC_LABEL_CONCATENATION = xy
    -> xy.f0+ xy.f1;
  private static final GeneralEdgeSemantics CONJUNCTIVE_SEMANTICS =  GeneralEdgeSemantics
    .fromEdgePredefinedSemantics(x -> y -> true,CONJUNCTIVE_BASICS,BASIC_LABEL_CONCATENATION);
  private static final GeneralEdgeSemantics CONJUNCTIVE_SEMANTICS_SAME_VERTEX_LABEL =
    GeneralEdgeSemantics
    .fromEdgePredefinedSemantics(x -> y -> true,CONJUNCTIVE_BASICS,BASIC_LABEL_CONCATENATION);

  /**
   * joining empties shall not return errors
   * @throws Exception
   */
  @Test
  public void empty_empty_empty() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("empty[]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("empty");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("empty");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("empty")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  /**
   * Do concatenate the labels for matching graphs
   * @throws Exception
   */
  @Test
  public void emptyG_emptyG_emptyGG() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyG:G[] " +
      "emptyGG:GG[]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyG");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyG");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyGG")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  /**
   * If two vertices have different values for the same property, then they shall not match
   * @throws Exception
   */
  @Test
  public void emptyV1_emptyV2_emptyGG() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyV1 {v: \"1\"}[] " +
        "emptyV2 {v: \"2\"}[] " +
        "emptyR {}[]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyV1");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyV2");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyR")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  /**
   * Joining matching graphs will also result into a LogicalGraph join part
   * @throws Exception
   */
  @Test
  public void empty_emptyVertex_to_empty() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString(
        "empty[] " +
        " emptyVertex {emptyVertex : \"graph\"}[()] " +
        " emptyWithLabel {emptyVertex : \"graph\"}[]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("empty");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyVertex");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyWithLabel")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  /**
   * Empty vertex matches (if always-true predicate) with another element
   * @throws Exception
   */
  @Test
  public void emptyVertex_simpleVertex_simpleVertexEx() throws Exception {
    FlinkAsciiGraphLoader loader =
      getLoaderFromString("empty[()] " +
        "simpleVertex {emptyVertex : \"graph\"}[(:Vertex {with: \"2\" })]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("empty");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("simpleVertex");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("simpleVertex")));
    collectAndAssertTrue(output.equalsByData(expected));
  }


  /**
   * Semantics on combining tuples for graph heads
   * @throws Exception
   */
  @Test
  public void emptyV1_emptyV2_emptyV3() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyV1 {v: \"1\", a: \"2\"}[] " +
        "emptyV2 {v: \"1\", b: \"3\"}[] " +
        "emptyR {v: \"1\", a: \"2\", b: \"3\"}[]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyV1");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyV2");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyR")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  /**
   * Semantics on combining tuples for graph heads
   * @throws Exception
   */
  @Test
  public void emptyV1_emptyV2_emptyV4() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyV1 {v: \"1\", a: \"2\"}[(:V1 {a: \"1\", b: \"2\"})] " +
        "emptyV2 {v: \"1\", b: \"3\"}[(:V1 {b: \"2\", c:\"3\"})] " +
        "emptyR {v: \"1\", a: \"2\", b: \"3\"}[(:V1V1 {a: \"1\", b: \"2\", c: \"3\"})]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyV1");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyV2");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyR")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyV1_emptyV2_joinMatch() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyV1 {v: \"1\", a: \"2\"}[(:V1 {a: \"1\", b: \"2\"})-[:alpha]->(:V2 {a: \"9\", b: " +
        "\"10\"})" +
        "] " +
        "emptyV2 {v: \"1\", b: \"3\"}[(:V3 {b: \"2\", c:\"3\"})-[:beta]->(:V4 {b: \"10\",c: " +
        "\"11\"})] " +
        "emptyR {v: \"1\", a: \"2\", b: \"3\"}[(:V1V3 {a: \"1\", b: \"2\", c: \"3\"})" +
        "-[:alphabeta]->(:V2V4 {a: \"9\", b:\"10\", c:\"11\"})]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyV1");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyV2");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyR")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyV1_emptyV2_joinNoMatch1() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyV1 {v: \"1\", a: \"2\"}[(:V1 {a: \"1\", b: \"2\"})-[:alpha]->(:V2 {a: \"9\", b: " +
        "\"10\"})" +
        "] " +
        "emptyV2 {v: \"1\", b: \"3\"}[(:V3 {b: \"2\", c:\"3\"})-[:beta]->(:V4 {b: \"12\",c: " +
        "\"11\"})] " +
        "emptyR {v: \"1\", a: \"2\", b: \"3\"}[(:V1V3 {a: \"1\", b: \"2\", c: \"3\"}))]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyV1");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyV2");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyR")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyV1_emptyV2_joinNoMatch2() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyV1 {v: \"1\", a: \"2\"}[(:V1 {a: \"1\", b: \"2\"})-[:alpha]->(:V2 {a: \"9\", b: " +
        "\"10\"})" +
        "] " +
        "emptyV2 {v: \"1\", b: \"3\"}[(:V3 {b: \"12\", c:\"3\"})-[:beta]->(:V4 {b: \"10\",c: " +
        "\"11\"})] " +
        "emptyR {v: \"1\", a: \"2\", b: \"3\"}[(:V2V4 {a: \"9\", b:\"10\", c:\"11\"})]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyV1");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyV2");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyR")));
    collectAndAssertTrue(output.equalsByData(expected));
  }

  @Test
  public void emptyV1_emptyV2_joinNoMatch3() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "emptyV1 {v: \"1\", a: \"2\"}[(:V1 {a: \"1\", b: \"2\"})-[:alpha]->(:V2 {a: \"9\", b: " +
        "\"10\"})" +
        "] " +
        "emptyV2 {v: \"1\", b: \"3\"}[(:V3 {b: \"12\", c:\"3\"})-[:beta]->(:V4 {b: \"14\",c: " +
        "\"11\"})] " +
        "emptyR {v: \"1\", a: \"2\", b: \"3\"}[]");
    LogicalGraph leftOperand = loader.getLogicalGraphByVariable("emptyV1");
    LogicalGraph rightOperand = loader.getLogicalGraphByVariable("emptyV2");
    GraphThetaJoin f = new GraphThetaJoin(GRAPH_INNER_JOIN, CONJUNCTIVE_SEMANTICS);
    LogicalGraph output = f.execute(leftOperand, rightOperand);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("emptyR")));
    collectAndAssertTrue(output.equalsByData(expected));
  }


}
