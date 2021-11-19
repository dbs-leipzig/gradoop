/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProjectEmbeddingsNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingMetaData inputMetaData = new EmbeddingMetaData();
    inputMetaData.setEntryColumn("a", EntryType.VERTEX, 0);
    inputMetaData.setEntryColumn("b", EntryType.VERTEX, 1);
    inputMetaData.setPropertyColumn("a", "age", 0);
    inputMetaData.setPropertyColumn("b", "name", 1);

    PlanNode mockNode = new MockPlanNode(null, inputMetaData);
    List<Pair<String, String>> projectedKeys = new ArrayList<>();
    projectedKeys.add(Pair.of("a", "age"));

    ProjectEmbeddingsNode node = new ProjectEmbeddingsNode(mockNode, projectedKeys);
    EmbeddingMetaData outputMetaData = node.getEmbeddingMetaData();
    assertEquals(2, outputMetaData.getEntryCount());
    assertEquals(0, outputMetaData.getEntryColumn("a"));
    assertEquals(1, outputMetaData.getEntryColumn("b"));
    assertEquals(1, outputMetaData.getPropertyCount());
    assertEquals(0, outputMetaData.getPropertyColumn("a", "age"));
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

    assertEquals(1, result.size());
    assertEquals(result.get(0).getId(0), vertexAId);
    assertEquals(result.get(0).getId(1), vertexBId);
    assertEquals(1, result.get(0).getProperties().size());
    assertEquals(result.get(0).getProperty(0), PropertyValue.create(42));
  }
}
