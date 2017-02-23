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

package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData.EntryType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class CNFTest {

  @Test
  public void testCopyConstructor() {
    CNF base = getPredicate("MATCH (a),(b) WHERE (a=b) AND (a = 0)");
    CNF copy = new CNF(base);
    copy.getPredicates().remove(0);

    assertNotEquals(base,copy);
  }

  @Test
  public void andConjunctionTest() {
    CNFElement or1 = new CNFElement();
    CNFElement or2 = new CNFElement();

    CNF and1 = new CNF();
    and1.addPredicate(or1);

    CNF and2 = new CNF();
    and2.addPredicate(or2);

    ArrayList<CNFElement> reference = new ArrayList<>();
    reference.add(or1);

    assertEquals(reference, and1.and(and2).and(and1).getPredicates());
  }

  @Test
  public void orConjunctionTest() {
    QueryHandler query = new QueryHandler("MATCH (a),(b),(c) WHERE a=b AND b=c");
    CNF cnf1 = query.getPredicates();

    query = new QueryHandler("MATCH (n),(m),(o) WHERE n=m AND m=o");
    CNF cnf2 = query.getPredicates();

    assertEquals(
      "((a = b OR n = m) AND (a = b OR m = o) AND (b = c OR n = m) AND (b = c OR m = o))",
      cnf1.or(cnf2).toString()
    );
  }

  @Test
  public void extractVariablesTest() {
    QueryHandler query = new QueryHandler("MATCH (a), (b) WHERE a=b OR a.name = \"Alice\"");
    CNF cnf = query.getPredicates();

    Set<String> reference = new HashSet<>();
    reference.add("a");
    reference.add("b");

    assertEquals(reference,cnf.getVariables());
  }

  @Test
  public void createExistingSubCnfTest() {
    CNF base = getPredicate(
      "MATCH (a), (b), (c) WHERE a=b AND a.name = \"Alice\" AND c.name = \"Chris\""
    );

    CNF expectedReturnValue = getPredicate(
      "MATCH (a), (b), (c) WHERE a.name = \"Alice\" AND c.name = \"Chris\""
    );

    assertEquals(expectedReturnValue, base.getSubCNF(Sets.newHashSet("a","c")));
    assertEquals(new CNF(), base.getSubCNF(Sets.newHashSet("b")));
  }

  @Test
  public void testRemoveCovered() {
    CNF base = getPredicate(
      "MATCH (a), (b), (c) WHERE (a = b) And (b > 5 OR a > 10) AND (c = false) AND (a = c)"
    );

    CNF expectedReturnValue = getPredicate(
      "MATCH (a), (b), (c) WHERE (a = b) And (b > 5 OR a > 10)"
    );

    CNF expectedNewBase = getPredicate(
      "MATCH (a), (b), (c) WHERE (c = false) AND (a = c)"
    );


    assertEquals(expectedReturnValue, base.removeSubCNF(Sets.newHashSet("a","b")));
    assertEquals(expectedNewBase, base);
  }

  @Test
  public void testEvaluationForProperties() {
    String queryString = "MATCH (a), (b), (c)" +
      "WHERE a=b AND a.name = \"Alice\" AND a.age > c.age";
    QueryHandler query = new QueryHandler(queryString);
    CNF cnf = query.getPredicates();

    GradoopId a = GradoopId.get();
    GradoopId c = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(
      a,
      Lists.newArrayList(PropertyValue.create("Alice"), PropertyValue.create(42))
    );

    embedding.add(a);
    embedding.add(
      c,
      Lists.newArrayList(PropertyValue.create(23))
    );

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setEntryColumn("c", EntryType.VERTEX, 2);
    metaData.setPropertyColumn("a", "name", 0);
    metaData.setPropertyColumn("a", "age", 1);
    metaData.setPropertyColumn("c", "age", 2);

    assertTrue(cnf.evaluate(embedding, metaData));
  }

  @Test public void testEvaluationWithMissingProperty() {
    String queryString = "MATCH (a) WHERE a.age > 20";
    QueryHandler query = new QueryHandler(queryString);
    CNF predicates = query.getPredicates();

    Embedding embedding = new Embedding();
    embedding.add(
      GradoopId.get(),
      Lists.newArrayList(PropertyValue.NULL_VALUE)
    );

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setPropertyColumn("a", "age", 0);

    assertFalse(predicates.evaluate(embedding, metaData));
  }

  @Test
  public void testGraphElementEvaluationForProperties() {
    String queryString = "MATCH (a) WHERE a.name = \"Alice\" AND a.__label__=\"Person\"";
    QueryHandler query = new QueryHandler(queryString);
    CNF cnf = query.getPredicates();

    Properties properties = new Properties();
    properties.set("name","Alice");
    Vertex vertex = new VertexFactory().createVertex("Person", properties);
    assertTrue(cnf.evaluate(vertex));

    properties.set("name","Bob");
    assertFalse(cnf.evaluate(vertex));
  }

  @Test public void testGraphElementEvaluationWithMissingProperty() {
    String queryString = "MATCH (a) WHERE a.name = \"Alice\" AND __label__=\"Person\"";
    QueryHandler query = new QueryHandler(queryString);
    CNF cnf = query.getPredicates();

    Properties properties = new Properties();
    properties.set("age",42);
    Vertex vertex = new VertexFactory().createVertex("Person", properties);
    assertFalse(cnf.evaluate(vertex));
  }

  private CNF getPredicate(String queryString) {
    QueryHandler query = new QueryHandler(queryString);
    return query.getPredicates();
  }
}
