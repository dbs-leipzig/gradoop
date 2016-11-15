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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterTest {

  @Test
  public void testReturnTrueIfPredicateHolds() {
    String query = "MATCH (a), (b), (c) WHERE a = b AND ( b.name = \"Bob\" OR b.age < c.age )";
    CNF predicates = new QueryHandler(query).getPredicates();

    PropertyList propertiesA = PropertyList.create();
    propertiesA.set("age", 42);

    PropertyList propertiesB = PropertyList.create();
    propertiesB.set("age", 23);
    propertiesB.set("name", "Bob");

    GradoopId aID = GradoopId.get();

    Embedding embedding = new Embedding(
      Lists.newArrayList(
        new IdEntry(aID),
        new ProjectionEntry(aID, propertiesB),
        new ProjectionEntry(GradoopId.get(), propertiesA)
      )
    );

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",1);
    columnMapping.put("c",2);

    assertTrue(Filter.filter(predicates, embedding, columnMapping));
  }

  @Test
  public void testFalseTrueIfPredicateHolds() {
    String query = "MATCH (a), (b), (c) WHERE a = b AND b.name = \"Bob\" OR b.age < c.age";
    CNF predicates = new QueryHandler(query).getPredicates();

    PropertyList propertiesA = PropertyList.create();
    propertiesA.set("age", 42);

    PropertyList propertiesB = PropertyList.create();
    propertiesB.set("age", 66);
    propertiesB.set("name", "Birgit");

    Embedding embedding = new Embedding(
      Lists.newArrayList(
        new IdEntry(GradoopId.get()),
        new ProjectionEntry(GradoopId.get(), propertiesB),
        new ProjectionEntry(GradoopId.get(), propertiesA)
      )
    );

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",1);
    columnMapping.put("c",2);

    assertFalse(Filter.filter(predicates, embedding, columnMapping));
  }
}
