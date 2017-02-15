package org.gradoop.flink.model.impl.operators.join;

import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.GraphThetaJoinWithJoins;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.edgesemantics.GeneralEdgeSemantics;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.edgesemantics.PredefinedEdgeSemantics;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by vasistas on 01/02/17.
 */
public class JoinTest_DisjGraphQExample extends GradoopFlinkTestBase {

  private static JoinType GRAPH_INNER_JOIN = JoinType.INNER;
  private static final PredefinedEdgeSemantics DISJUNCTIVE_BASICS
    = PredefinedEdgeSemantics.DISJUNCTIVE;
  private static final Function<Tuple2<String,String>,String> BASIC_LABEL_CONCATENATION
    = xy -> xy.f0 + xy.f1;
  private static final GeneralEdgeSemantics DISJUNCTIVE_SEMANTICS =  GeneralEdgeSemantics
    .fromEdgePredefinedSemantics(x -> y -> true, DISJUNCTIVE_BASICS,BASIC_LABEL_CONCATENATION);

  /**
   * Defining the hashing functions required to break down the join function
   */
  private static final Function<Vertex, Long> leftHash = v -> (long)v.getPropertyValue("name")
  .getString().hashCode();
  private static final Function<Vertex, Long> rightHash = v -> (long)v.getPropertyValue("fstAuth")
    .getString().hashCode();
  private static final Function<Vertex,Function<Vertex,Boolean>> thetaVertex = x -> y -> x
    .getPropertyValue("name").equals(y.getPropertyValue("fstAuth"));

  private static final String researchGateGraph = "research:RG{}[" +
    "(a1:User {name: \"Alice\"}) " +
    "(a2:User {name: \"Bob\"}) " +
    "(a3:User {name: \"Carl\"}) " +
    "(a4:User {name: \"Dan\"}) " +
    "(a1)-[:follows]->(a2) " +
    "(a1)-[:follows]->(a3) " +
    "(a2)-[:follows]->(a4) " +
    "(a4)-[:follows]->(a3) " +
    "]";

  private static final String citationGraph = "citation:CG{}[" +
    "(p1:Paper {title: \"Graphs\", fstAuth: \"Alice\"}) " +
    "(p2:Paper {title: \"Join\", fstAuth: \"Alice\"}) " +
    "(p3:Paper {title: \"OWL\", fstAuth: \"Bob\"}) " +
    "(p4:Paper {title: \"Projection\", fstAuth: \"Carl\"}) " +
    "(p5:Paper {title: \"muCalc\", fstAuth: \"Dan\"}) " +
    "(p1)-[:cites]->(p3) " +
    "(p2)-[:cites]->(p4) " +
    "(p3)-[:cites]->(p4) " +
    "(p4)-[:cites]->(p5) " +
    "]";

  private static final String joinedDisjunctively = "result:RGCG{}[" +
    "(r1:UserPaper {title: \"Graphs\", fstAuth: \"Alice\", name: \"Alice\"}) " +
    "(r2:UserPaper {title: \"Join\", fstAuth: \"Alice\", name: \"Alice\"}) " +
    "(r3:UserPaper {title: \"OWL\", fstAuth: \"Bob\", name: \"Bob\"}) " +
    "(r4:UserPaper {title: \"Projection\", fstAuth: \"Carl\", name: \"Carl\"}) " +
    "(r5:UserPaper {title: \"muCalc\", fstAuth: \"Dan\", name: \"Dan\"}) " +
    "(r1)-[:followscites]->(r3) " +
    "(r2)-[:followscites]->(r4) " +
    "(r3)-[:cites]->(r4) " +
    "(r4)-[:cites]->(r5) " +
    "(r1)-[:follows]->(r4) " +
    "(r2)-[:follows]->(r3) " +
    "(r3)-[:follows]->(r5) " +
    "(r5)-[:follows]->(r4) " +
    "]";

  /**
   * joining empties shall not return errors
   * @throws Exception
   */
  @Test
  public void paper_disjunctive_example() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(researchGateGraph+" "+citationGraph+" "+
      joinedDisjunctively);
    LogicalGraph searchGraph = loader.getLogicalGraphByVariable("research");
    LogicalGraph patternGraph = loader.getLogicalGraphByVariable("citation");
    GraphThetaJoinWithJoins f = new GraphThetaJoinWithJoins(GRAPH_INNER_JOIN, DISJUNCTIVE_SEMANTICS,
      leftHash, rightHash, thetaVertex, null, null, null);
    LogicalGraph output = f.execute(searchGraph, patternGraph);
    LogicalGraph expected = ((loader.getLogicalGraphByVariable("result")));
    collectAndAssertTrue(output.equalsByData(expected));
  }



}
