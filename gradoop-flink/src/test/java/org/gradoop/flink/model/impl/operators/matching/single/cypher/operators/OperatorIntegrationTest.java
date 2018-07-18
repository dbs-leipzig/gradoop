/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//
//package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import org.apache.flink.api.common.operators.base.JoinOperatorBase;
//import org.apache.flink.api.java.DataSet;
//import org.gradoop.flink.model.GradoopFlinkTestBase;
//import org.gradoop.flink.model.impl.LogicalGraph;
//import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
//import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
//import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
//import Embedding;
//import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.*;
//import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.cartesian
//  .CartesianProduct;
//import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.Expand;
//import org.gradoop.flink.util.FlinkAsciiGraphLoader;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//
//import static org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection.*;
//
//@Ignore
//public class OperatorIntegrationTest extends GradoopFlinkTestBase {
//
//  private FlinkAsciiGraphLoader testGraphs;
//
//  @Before
//  public void loadGraph() throws Exception {
//    this.testGraphs = getLoaderFromFile(
//      OperatorIntegrationTest.class.getResource("/data/gdl/physicalOperators.gdl").getFile()
//    );
//  }
//
//  /**
//   * MATCH (p1:Person {name: "Alice"})-[r1:worked_at]->(o)
//   * WHERE r1.active=1
//   * RETURN *
//   */
//  @Test
//  public void simplePredicateQueryTest() throws Exception {
//    QueryHandler query = new QueryHandler(
//      "MATCH (p1:Person {name: \"Alice\"})-[r1:worked_at]->(o) " +
//      "WHERE r1.active=1"
//    );
//
//    LogicalGraph graph = testGraphs.getLogicalGraphByVariable("g");
//
//    CNF predicates = query.getPredicates();
//    CNF p1Predicate = predicates.getSubCNF(Sets.newHashSet("p1"));
//    CNF r1Predicate = predicates.getSubCNF(Sets.newHashSet("r1"));
//
//    DataSet<Embedding> p1 =
//      new FilterVertices(graph.getVertices(), p1Predicate).evaluate();
//    DataSet<Embedding> r1 =
//      new FilterEdges(graph.getEdges(), r1Predicate).evaluate();
//
//    //DataSet<Embedding> res = new  JoinEmbeddings(p1,r1,0, ExpandDirection.OUT).evaluate();
//
//    //System.out.println("res.collect() = " + res.collect());
//  }
//
//  /**
//   * MATCH (a:Person)-[]->(b:Person)
//   * WHERE a.age > b.age OR a.name=b.name
//   * RETURN *
//   */
//  @Test
//  public void CrossPredicateTest() throws Exception {
//    LogicalGraph graph = loadGraph("","g");
//
//    QueryHandler query = new QueryHandler(
//      "MATCH (a:Person)-[]->(b:Person)" +
//      "WHERE a.age > b.age OR a.name=b.name"
//    );
//
//    CNF predicates = query.getPredicates();
//
//    DataSet<Embedding> vertices =
//      new FilterAndProjectVertices(
//          graph.getVertices(),
//          predicates.getSubCNF(Sets.newHashSet("a")),
//          Lists.newArrayList("age","name")
//      ).evaluate();
//
//    DataSet<Embedding> edges = new ProjectEdges(graph.getEdges()).evaluate();
//
//    DataSet<Embedding> aExpanded = new ExpandOne(vertices,edges,0, OUT).evaluate();
//
//    DataSet<Embedding> ab = new JoinEmbeddings(aExpanded,vertices, 2, 0).evaluate();
//
//    HashMap<String, Integer> mapping = new HashMap<>();
//    mapping.put("a",0);
//    mapping.put("b",2);
//    DataSet<Embedding> res = new FilterEmbeddings(
//      ab,
//      predicates.getSubCNF(Sets.newHashSet("a","b")),
//      mapping
//    ).evaluate();
//
//    System.out.println("res.collect() = " + res.collect());
//  }
//
//  /**
//   * MATCH (n)-[]->(m)-[]->(o)
//   * RETURN *
//   */
//  @Test
//  public void homomorphismTest() throws Exception {
//    LogicalGraph graph = loadGraph("","g");
//
//    DataSet<Embedding> vertices =
//      new ProjectVertices(graph.getVertices()).evaluate();
//    DataSet<Embedding> edges =
//      new ProjectEdges(graph.getEdges()).evaluate();
//
//    DataSet<Embedding> a = new ExpandOne(
//      vertices, edges, 0,
//      OUT, MatchStrategy.HOMOMORPHISM, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
//    ).evaluate();
//
//    DataSet<Embedding> res = new ExpandOne(a, edges, 2,
//      OUT, MatchStrategy.HOMOMORPHISM, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
//    ).evaluate();
//
//    System.out.println("res.collect() = " + res.collect());
//  }
//
//  /**
//   * MATCH (n)-[]->(m)-[]->(o)
//   * RETURN *
//   */
//  @Test
//  public void isomorphismTest() throws Exception {
//    LogicalGraph graph = loadGraph("","g");
//
//    DataSet<Embedding> vertices =
//      new ProjectVertices(graph.getVertices()).evaluate();
//    DataSet<Embedding> edges =
//      new ProjectEdges(graph.getEdges()).evaluate();
//
//    DataSet<Embedding> a = new ExpandOne(
//      vertices, edges, 0,
//      OUT, MatchStrategy.ISOMORPHISM, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
//    ).evaluate();
//
//    DataSet<Embedding> res = new ExpandOne(a, edges, 2,
//      OUT, MatchStrategy.ISOMORPHISM, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
//    ).evaluate();
//
//    System.out.println("res.collect() = " + res.collect());
//  }
//
//  /**
//   * MATCH (n)-[*2..3]->(m)
//   * RETURN *
//   */
//  @Test
//  public void variableLengthPathQueryTest() throws Exception{
//    LogicalGraph graph = loadGraph("","g");
//
//    DataSet<Embedding> n = new ProjectVertices(graph.getVertices()).evaluate();
//    DataSet<Embedding> edges = new ProjectEdges(graph.getEdges()).evaluate();
//
//    DataSet<Embedding> res =
//      new Expand(n,edges,0,2,3,OUT, new ArrayList<>(), new ArrayList<>(),-1).evaluate();
//
//    System.out.println("res.collect() = " + res.collect());
//  }
//
//  /**
//   * MATCH (a:Department), (b:City)
//   * RETURN *
//   */
//  @Test
//  public void cartesianProductTest() throws Exception {
//    LogicalGraph graph = loadGraph("","g");
//    QueryHandler query = new QueryHandler("MATCH (a:Department), (b:City)");
//    CNF predicates = query.getPredicates();
//
//    DataSet<Embedding> a = new FilterVertices(
//      graph.getVertices(),
//      predicates.getSubCNF(Sets.newHashSet("a"))
//    ).evaluate();
//    DataSet<Embedding> b = new FilterVertices(
//      graph.getVertices(),
//      predicates.getSubCNF(Sets.newHashSet("a"))
//    ).evaluate();
//
//    DataSet<Embedding> res = new CartesianProduct(a,b).evaluate();
//
//    System.out.println("res.collect() = " + res.collect());
//  }
//
//  /**
//   * ⁠MATCH (a:Department), (b)-[]->(c:Person {name: "Alice")
//   * WHERE a.prop = b.prop
//   * RETURN *
//   */
//  @Test
//  public void valueJoinTest() throws Exception {
//    LogicalGraph graph = loadGraph("","g");
//    QueryHandler query = new QueryHandler(
//      " MATCH (a:Department),(b)-[]->(c:Person {name: \"Alice\")" +
//      " WHERE a.prop = b.prop"
//    );
//    CNF predicates = query.getPredicates();
//
//    DataSet<Embedding> a =
//      new FilterAndProjectVertices(
//        graph.getVertices(),
//        predicates.getSubCNF(Sets.newHashSet("a")),
//        Lists.newArrayList("prop")
//      ).evaluate();
//
//    DataSet<Embedding> c = new FilterVertices(
//      graph.getVertices(),
//      predicates.getSubCNF(Sets.newHashSet("c"))
//    ).evaluate();
//
//    DataSet<Embedding> edges = new ProjectEdges(graph.getEdges()).evaluate();
//
//    DataSet<Embedding> cexpand = new ExpandOne(c,edges,0,IN).evaluate();
//
//    DataSet<Embedding> b =
//      new ProjectVertices(graph.getVertices(), Lists.newArrayList("prop")).evaluate();
//
//    DataSet<Embedding> bc = new JoinEmbeddings(cexpand,b,2,0).evaluate();
//
//    HashMap<String, Integer> mapping = new HashMap<>();
//    mapping.put("a",0);
//    mapping.put("b",3);
//
//    DataSet<Embedding> res = new ValueJoin(
//      a,
//      bc,
//      predicates.getSubCNF(Sets.newHashSet("a","b")),
//      mapping
//    ).evaluate();
//
//    System.out.println("res.collect() = " + res.collect());
//  }
//
//   private LogicalGraph loadGraph(String dataGraph, String variable) {
//    FlinkAsciiGraphLoader loader = getLoaderFromString(dataGraph);
//    return loader.getLogicalGraphByVariable(variable);
//  }
//}
