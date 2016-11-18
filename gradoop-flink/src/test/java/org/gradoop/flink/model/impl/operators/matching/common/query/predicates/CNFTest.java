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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class CNFTest {
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
    reference.add(or2);

    assertEquals(reference, and1.and(and2).getPredicates());
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
    String queryString = "MATCH (a), (b), (c)" +
      "WHERE a=b AND a.name = \"Alice\" AND c.name = \"Chris\"";
    QueryHandler query = new QueryHandler(queryString);
    CNF cnf = query.getPredicates();

    Set<String> variables = new HashSet<>();
    variables.add("a");
    variables.add("b");

    assertEquals("((a = b))",cnf.getSubCNF(variables).toString());

    variables.add("c");
    assertTrue(cnf.getSubCNF(variables).getPredicates().isEmpty());
  }

  @Test
  public void testEvaluationForProperties() {
    String queryString = "MATCH (a), (b), (c)" +
      "WHERE a=b AND a.name = \"Alice\" AND a.age > c.age";
    QueryHandler query = new QueryHandler(queryString);
    CNF cnf = query.getPredicates();

    ProjectionEntry a = new ProjectionEntry(GradoopId.get());
    a.addProperty(Property.create("name","Alice"));
    a.addProperty(Property.create("age",42));

    ProjectionEntry c = new ProjectionEntry(GradoopId.get());
    c.addProperty(Property.create("age",23));

    HashMap<String, EmbeddingEntry> mapping = new HashMap<>();
    mapping.put("a",a);
    mapping.put("b",a);
    mapping.put("c",c);

    assertTrue(cnf.evaluate(mapping));

    c.addProperty(Property.create("age",101));

    assertFalse(cnf.evaluate(mapping));
  }

  @Test public void testEvaluationWithMissingProperty() {
    String queryString = "MATCH (a) WHERE a.age > 20";
    QueryHandler query = new QueryHandler(queryString);
    CNF predicates = query.getPredicates();

    ProjectionEntry a = new ProjectionEntry(GradoopId.get());
    a.addProperty(Property.create("name", "Alice"));

    HashMap<String, EmbeddingEntry> mapping = new HashMap<>();
    mapping.put("a",a);

    assertFalse(predicates.evaluate(mapping));
  }
}
