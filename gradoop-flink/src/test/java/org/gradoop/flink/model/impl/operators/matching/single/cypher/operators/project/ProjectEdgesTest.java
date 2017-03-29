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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.assertEveryEmbedding;
import static org.junit.Assert.assertEquals;

public class ProjectEdgesTest extends PhysicalOperatorTest {

  @Test
  public void returnsEmbeddingWithIdProjectionId() throws Exception{
    DataSet<Edge> edgeDataSet = createEdgesWithProperties(Lists.newArrayList("foo", "bar", "baz"));
    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");

    ProjectEdges operator = new ProjectEdges(edgeDataSet, extractedPropertyKeys, false);
    DataSet<Embedding> results = operator.evaluate();

    assertEquals(2,results.count());
    assertEveryEmbedding(results, (embedding) -> {
      assertEquals(3,embedding.size());
      assertEquals(PropertyValue.create("foo"), embedding.getProperty(0));
      assertEquals(PropertyValue.create("bar"), embedding.getProperty(1));
    });
  }

  @Test
  public void testProjectLoop() throws Exception {
    GradoopId a = GradoopId.get();
    Edge edge = new EdgeFactory().createEdge(a, a);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(edge);

    Embedding result = new ProjectEdges(edges, Collections.emptyList(), true)
      .evaluate().collect().get(0);

    assertEquals(result.size(), 2);
    assertEquals(a, result.getId(0));
    assertEquals(edge.getId(), result.getId(1));
  }
}
