/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.s1ck.gdl.model.cnf.CNF;
import org.s1ck.gdl.model.cnf.CNFElement;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.HashMap;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection.*;

public class OperatorIntegrationTest extends GradoopFlinkTestBase {


  /**
   * MATCH (p1:Person {name: "Alice"})-[r1:worked_at]->(o)
   * WHERE r1.active=1
   * RETURN *
   */
  @Test
  public void simplePredicateQueryTest() throws Exception {
    LogicalGraph graph = loadGraph("","g");

    CNF p1Predicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("p1","name"), Comparison.Comparator.EQ, new Literal("Alice")
        )
      )),
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("p1","label"), Comparison.Comparator.EQ, new Literal("Person")
        )
      ))
    ));

    CNF r1Predicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("r1","active"), Comparison.Comparator.EQ, new Literal(1)
        )
      )),
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("r1","label"), Comparison.Comparator.EQ, new Literal("worked_at")
        )
      ))
    ));

    DataSet<Embedding> p1 =
      new FilterVertices(graph.getVertices(), p1Predicate).evaluate();
    DataSet<Embedding> r1 =
      new FilterEdges(graph.getEdges(), r1Predicate).evaluate();
    DataSet<Embedding> res = new ExpandOne(p1,r1,0,OUT).evaluate();

    System.out.println("res.collect() = " + res.collect());
  }
​
​  /**
   * MATCH (a:Person)-[]->(b:Person)
   * WHERE a.age > b.age OR a.name=b.name
   * RETURN *
   */
  @Test
  public void CrossPredicateTest() throws Exception {
    LogicalGraph graph = loadGraph("","g");

    CNF vertexPredicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("a","label"), Comparison.Comparator.EQ, new Literal("Person")
        )
      ))
    ));

    CNF crossPredicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("a","age"), Comparison.Comparator.LT, new PropertySelector("b","age")
        ),
        new Comparison(
          new PropertySelector("a","name"), Comparison.Comparator.EQ, new PropertySelector("b","name")
        )
      )),
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("b","label"), Comparison.Comparator.EQ, new Literal("Person")
        )
      ))
    ));


    DataSet<Embedding> vertices =
      new FilterAndProjectVertices(graph.getVertices(), vertexPredicate, Lists.newArrayList("age","name"))
        .evaluate();

    DataSet<Embedding> edges = new ProjectEdges(graph.getEdges(),Lists.newArrayList()).evaluate();

    DataSet<Embedding> aExpanded = new ExpandOne(vertices,edges,0,OUT).evaluate();

    DataSet<Embedding> ab = new JoinEmbeddings(aExpanded,vertices,2,0).evaluate();

    HashMap<String, Integer> mapping = new HashMap<>();
    mapping.put("a",0);
    mapping.put("b",2);
    DataSet<Embedding> res = new FilterEmbeddings(ab, crossPredicate, mapping).evaluate();

    System.out.println("res.collect() = " + res.collect());
  }

​
  /**
   * MATCH (n)-[]->(m)-[]->(o)
   * RETURN *
   */
​  @Test
  public void homomorphismTest() throws Exception {
    LogicalGraph graph = loadGraph("","g");

    DataSet<Embedding> vertices =
      new ProjectVertices(graph.getVertices(), Lists.newArrayList()).evaluate();
    DataSet<Embedding> edges =
      new ProjectEdges(graph.getEdges(), Lists.newArrayList()).evaluate();

    DataSet a = new ExpandOne(vertices, edges, 0, OUT).evaluate();

    DataSet res = new ExpandOne(a, edges, 2, OUT).evaluate();

    System.out.println("res.collect() = " + res.collect());
  }

  /**
   * MATCH (n)-[]->(m)-[]->(o)
   * RETURN *
   */
​  @Test
  public void isomorphismTest() throws Exception {
    LogicalGraph graph = loadGraph("","g");

    CNF isomorphismCheck = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Not(new Comparison(
          new ElementSelector("n"), Comparison.Comparator.LT, new ElementSelector("m")
        ))
      )),
      new CNFElement(Lists.newArrayList(
        new Not(new Comparison(
          new ElementSelector("n"), Comparison.Comparator.LT, new ElementSelector("o")
        ))
      )),
      new CNFElement(Lists.newArrayList(
        new Not(new Comparison(
          new ElementSelector("m"), Comparison.Comparator.LT, new ElementSelector("o")
        ))
      )),
      new CNFElement(Lists.newArrayList(
        new Not(new Comparison(
          new ElementSelector("e1"), Comparison.Comparator.LT, new ElementSelector("e2")
        ))
      ))
    ));

    DataSet<Embedding> vertices =
      new ProjectVertices(graph.getVertices(),Lists.newArrayList()).evaluate();
    DataSet<Embedding> edges =
      new ProjectEdges(graph.getEdges(),Lists.newArrayList()).evaluate();
    DataSet<Embedding> a = new ExpandOne(vertices,edges,0,OUT).evaluate();
    DataSet<Embedding> b = new ExpandOne(a,edges,2,OUT).evaluate();

    HashMap<String, Integer> mapping = new HashMap<>();
    mapping.put("n",0);
    mapping.put("m",2);
    mapping.put("o",4);
    mapping.put("e1",1);
    mapping.put("e1",3);

    DataSet<Embedding> res = new FilterEmbeddings(b,isomorphismCheck, mapping).evaluate();

    System.out.println("res.collect() = " + res.collect());
  }

  /**
   * MATCH (n)-[*2..3]->(m)
   * RETURN *
   */
  @Test
  public void variableLengthPathQueryTest() throws Exception{
    LogicalGraph graph = loadGraph("","g");

    DataSet<Embedding> n =
      new ProjectVertices(graph.getVertices(), Lists.newArrayList()).evaluate();
    DataSet<Embedding> edges =
      new ProjectEdges(graph.getEdges(), Lists.newArrayList()).evaluate();

    DataSet<Embedding> res = new Expand(n,edges,0,2,3,OUT).evaluate();

    System.out.println("res.collect() = " + res.collect());
  }
​

  /**
   * MATCH (a:Department), (b:City)
   * RETURN *
   */
  @Test
  public void cartesianProductTest() throws Exception {
    LogicalGraph graph = loadGraph("","g");

    CNF aPredicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("a","label"), Comparison.Comparator.EQ, new Literal("Department")
        )
      ))
    ));

    CNF bPredicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("b","label"), Comparison.Comparator.EQ, new Literal("City")
        )
      ))
    ));

    DataSet<Embedding> a = new FilterVertices(graph.getVertices(), aPredicate).evaluate();
    DataSet<Embedding> b = new FilterVertices(graph.getVertices(), bPredicate).evaluate();
    DataSet<Embedding> res = new CartesianProduct(a,b).evaluate();

    System.out.println("res.collect() = " + res.collect());
  }

  /**
   * ⁠MATCH (a:Department), (b)-[]->(c:Person {name: "Alice")
   * WHERE a.prop = b.prop
   * RETURN *
   */
  @Test
  public void valueJoinTest() throws Exception {
    LogicalGraph graph = loadGraph("","g");

    CNF aPredicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("a","label"), Comparison.Comparator.EQ, new Literal("Department")
        )
      ))
    ));

    DataSet<Embedding> a =
      new FilterAndProjectVertices(graph.getVertices(), aPredicate, Lists.newArrayList("prop")).evaluate();

    CNF cPredicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("c","label"), Comparison.Comparator.EQ, new Literal("Person")
        )
      )),
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("c","name"), Comparison.Comparator.EQ, new Literal("Alice")
        )
      ))
    ));

    DataSet<Embedding> c = new FilterVertices(graph.getVertices(),cPredicate).evaluate();

    DataSet<Embedding> edges = new ProjectEdges(graph.getEdges(), Lists.newArrayList()).evaluate();

    DataSet<Embedding> cexpand = new ExpandOne(c,edges,0,IN).evaluate();

    DataSet<Embedding> b =
      new ProjectVertices(graph.getVertices(), Lists.newArrayList("prop")).evaluate();

    DataSet<Embedding> bc = new JoinEmbeddings(cexpand,b,2,0).evaluate();

    CNF joinPredicate = new CNF(Lists.newArrayList(
      new CNFElement(Lists.newArrayList(
        new Comparison(
          new PropertySelector("a","prop"), Comparison.Comparator.EQ, new PropertySelector("b","prop")
        )
      ))
    ));

    HashMap<String, Integer> mapping = new HashMap<>();
    mapping.put("a",0);
    mapping.put("b",3);

    DataSet<Embedding> res = new ValueJoin(a, bc, joinPredicate, mapping).evaluate();

    System.out.println("res.collect() = " + res.collect());
  }

  private LogicalGraph loadGraph(String dataGraph, String variable) {
    FlinkAsciiGraphLoader loader = getLoaderFromString(dataGraph);
    return loader.getLogicalGraphByVariable(variable);
  }
}
