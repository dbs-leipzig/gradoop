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
