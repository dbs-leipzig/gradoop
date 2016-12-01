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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.ProjectionEntry;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectEmbeddingsTest extends PhysicalOperatorTest {

  @Test
  public void testFilterEmbeddings() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    Properties propertiesA = Properties.create();
    propertiesA.set("age", 23);

    Properties propertiesB = Properties.create();
    propertiesB.set("age", 42);

    DataSet<Embedding> embedding = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new Embedding(Lists.newArrayList(
          new ProjectionEntry(GradoopId.get(), propertiesA),
          new ProjectionEntry(GradoopId.get(), propertiesB)
        ))
      )
    );

    Map<Integer,List<String>> propertyKeyMapping = new HashMap<>();
    propertyKeyMapping.put(0,Lists.newArrayList("age"));
    propertyKeyMapping.put(1,Lists.newArrayList("age"));

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",1);

    FilterAndProjectEmbeddings filter = new FilterAndProjectEmbeddings(
      embedding, predicates, columnMapping, propertyKeyMapping);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testKeepEmbeddings() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    Properties propertiesA = Properties.create();
    propertiesA.set("age", 42);

    Properties propertiesB = Properties.create();
    propertiesB.set("age", 23);

    DataSet<Embedding> embedding = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new Embedding(Lists.newArrayList(
          new ProjectionEntry(GradoopId.get(), propertiesA),
          new ProjectionEntry(GradoopId.get(), propertiesB)
        ))
      )
    );

    Map<Integer,List<String>> propertyKeyMapping = new HashMap<>();
    propertyKeyMapping.put(0,Lists.newArrayList("age"));
    propertyKeyMapping.put(1,Lists.newArrayList("age"));

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",1);

    FilterAndProjectEmbeddings filter = new FilterAndProjectEmbeddings(
      embedding, predicates, columnMapping, propertyKeyMapping);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testProjectRemainingEmbeddings() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a),(b) WHERE a.age > b.age");

    Properties propertiesA = Properties.create();
    propertiesA.set("age", 42);
    propertiesA.set("foo", "bar");

    Properties propertiesB = Properties.create();
    propertiesB.set("age", 23);
    propertiesB.set("hello", "world");

    DataSet<Embedding> embedding = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new Embedding(Lists.newArrayList(
          new ProjectionEntry(GradoopId.get(), propertiesA),
          new ProjectionEntry(GradoopId.get(), propertiesB)
        )),
        new Embedding(Lists.newArrayList(
          new ProjectionEntry(GradoopId.get(), propertiesB),
          new ProjectionEntry(GradoopId.get(), propertiesA)
        ))
      )
    );

    Map<Integer,List<String>> propertyKeyMapping = new HashMap<>();
    propertyKeyMapping.put(0,Lists.newArrayList("foo"));
    propertyKeyMapping.put(1,Lists.newArrayList("hello"));

    Map<String,Integer> columnMapping = new HashMap<>();
    columnMapping.put("a",0);
    columnMapping.put("b",1);

    FilterAndProjectEmbeddings operator = new FilterAndProjectEmbeddings(
      embedding, predicates, columnMapping, propertyKeyMapping);

    List<Embedding> result = operator.evaluate().collect();

    assertEquals(1, result.size());
    assertEquals(ProjectionEntry.class, result.get(0).getEntry(0).getClass());
    assertEquals(ProjectionEntry.class, result.get(0).getEntry(1).getClass());

    assertEquals(
      Sets.newHashSet("foo"),
      result.get(0).getEntry(0).getProperties().get().getKeys()
    );

    assertEquals(
      Sets.newHashSet("hello"),
      result.get(0).getEntry(1).getProperties().get().getKeys()
    );
  }

}
