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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ProjectEmbeddingsNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingMetaData inputMetaData = new EmbeddingMetaData();
    inputMetaData.setEntryColumn("a", EntryType.VERTEX, 0);
    inputMetaData.setEntryColumn("b", EntryType.VERTEX, 1);
    inputMetaData.setPropertyColumn("a", "age", 0);
    inputMetaData.setPropertyColumn("b","name", 1);

    PlanNode mockNode = new MockPlanNode(null, inputMetaData);
    List<Pair<String, String>> projectedKeys = new ArrayList<>();
    projectedKeys.add(Pair.of("a", "age"));

    ProjectEmbeddingsNode node = new ProjectEmbeddingsNode(mockNode, projectedKeys);
    EmbeddingMetaData outputMetaData = node.getEmbeddingMetaData();
    assertThat(outputMetaData.getEntryCount(), is(2));
    assertThat(outputMetaData.getEntryColumn("a"), is(0));
    assertThat(outputMetaData.getEntryColumn("b"), is(1));
    assertThat(outputMetaData.getPropertyCount(), is(1));
    assertThat(outputMetaData.getPropertyColumn("a", "age"), is(0));
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId vertexAId = GradoopId.get();
    GradoopId vertexBId = GradoopId.get();

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("b", "age", 1);

    Embedding embedding1 = new Embedding();
    embedding1.add(vertexAId, PropertyValue.create(42));
    embedding1.add(vertexBId, PropertyValue.create(23));

    DataSet<Embedding> input = getExecutionEnvironment().fromElements(embedding1);
    PlanNode mockChild = new MockPlanNode(input, metaData);

    List<Pair<String, String>> projectedKeys = new ArrayList<>();
    projectedKeys.add(Pair.of("a", "age"));

    List<Embedding> result = new ProjectEmbeddingsNode(mockChild, projectedKeys).execute().collect();

    Assert.assertThat(result.size(), Is.is(1));
    assertTrue(result.get(0).getId(0).equals(vertexAId));
    assertTrue(result.get(0).getId(1).equals(vertexBId));
    assertThat(result.get(0).getProperties().size(), is(1));
    assertTrue(result.get(0).getProperty(0).equals(PropertyValue.create(42)));
  }
}
