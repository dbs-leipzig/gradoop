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
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterVerticesTest extends PhysicalOperatorTest {

  @Test
  public void testFilterVerticesByProperty() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Anton");
    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Person", properties)
      )
    );

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updatePropertyMapping("a", "name", 0);

    FilterVertices filter = new FilterVertices(vertex, predicates, metaData);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testFilterVerticesByLabel() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a:Person)");

    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Robot")
      )
    );

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updatePropertyMapping("a", "__label__", 0);

    FilterVertices filter = new FilterVertices(vertex, predicates, metaData);
    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testKeepVertices() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Person", properties)
      )
    );

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updatePropertyMapping("a", "name", 0);

    FilterVertices filter = new FilterVertices(vertex, predicates, metaData);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testKeepVerticesByLabel() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a:Person)");

    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Person")
      )
    );

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updatePropertyMapping("a", "__label__", 0);

    FilterVertices filter = new FilterVertices(vertex, predicates, metaData);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testReturnEmbeddingWithIdEntry() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Person", properties)
      )
    );

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.updateColumnMapping("a", 0);
    metaData.updatePropertyMapping("a", "name", 0);

    FilterVertices filter = new FilterVertices(vertex, predicates, metaData);

    List<Embedding> result = filter.evaluate().collect();

    assertEquals(vertex.collect().get(0).getId(), result.get(0).getId(0));
  }


}
