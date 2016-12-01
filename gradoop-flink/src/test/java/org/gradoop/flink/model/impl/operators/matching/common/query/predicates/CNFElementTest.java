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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.ProjectionEntry;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class CNFElementTest {
  @Test
  public void extractVariablesTest() {
    QueryHandler query = new QueryHandler("MATCH (a), (b) WHERE a=b OR a.name = \"Alice\"");
    CNFElement cnfElement = query.getPredicates().getPredicates().get(0);

    Set<String> reference = new HashSet<>();
    reference.add("a");
    reference.add("b");

    assertEquals(reference,cnfElement.getVariables());
  }

  @Test
  public void testEvaluation() {
    String queryString = "MATCH (a), (b), (c)" +
      "WHERE a=b OR a.name = \"Alice\" OR a.age > c.age";
    QueryHandler query = new QueryHandler(queryString);
    CNFElement cnfElement = query.getPredicates().getPredicates().get(0);

    ProjectionEntry a = new ProjectionEntry(GradoopId.get());
    a.addProperty(Property.create("name","Alfred"));
    a.addProperty(Property.create("age",42));
    ProjectionEntry c = new ProjectionEntry(GradoopId.get());
    c.addProperty(Property.create("age",42));
    HashMap<String, EmbeddingEntry> mapping = new HashMap<>();
    mapping.put("a",a);
    mapping.put("b",a);
    mapping.put("c",c);
    assertTrue(cnfElement.evaluate(mapping));

    a.addProperty(Property.create("name","Alice"));
    mapping.put("b",c);
    assertTrue(cnfElement.evaluate(mapping));

    a.addProperty(Property.create("name","Alfred"));
    c.addProperty(Property.create("age",23));
    assertTrue(cnfElement.evaluate(mapping));

    c.addProperty(Property.create("age",101));
    assertFalse(cnfElement.evaluate(mapping));
  }
}
