/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProjectTemporalEmbeddingsNodeTest {
  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingTPGMMetaData inputMetaData = new EmbeddingTPGMMetaData();
    inputMetaData.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    inputMetaData.setEntryColumn("b", EmbeddingTPGMMetaData.EntryType.VERTEX, 1);
    inputMetaData.setPropertyColumn("a", "age", 0);
    inputMetaData.setPropertyColumn("b", "name", 1);
    inputMetaData.setTimeColumn("a", 0);
    inputMetaData.setTimeColumn("b", 1);

    PlanNode mockNode = new MockPlanNode(null, inputMetaData);
    List<Pair<String, String>> projectedKeys = new ArrayList<>();
    projectedKeys.add(Pair.of("a", "age"));

    ProjectTemporalEmbeddingsNode node = new ProjectTemporalEmbeddingsNode(mockNode, projectedKeys);
    EmbeddingTPGMMetaData outputMetaData = node.getEmbeddingMetaData();
    assertThat(outputMetaData.getEntryCount(), is(2));
    assertThat(outputMetaData.getEntryColumn("a"), is(0));
    assertThat(outputMetaData.getEntryColumn("b"), is(1));
    assertThat(outputMetaData.getPropertyCount(), is(1));
    assertThat(outputMetaData.getPropertyColumn("a", "age"), is(0));
    assertThat(outputMetaData.getTimeColumn("a"), is(0));
    assertThat(outputMetaData.getTimeColumn("b"), is(1));
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId vertexAId = GradoopId.get();
    GradoopId vertexBId = GradoopId.get();

    EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
    metaData.setEntryColumn("a", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EmbeddingTPGMMetaData.EntryType.VERTEX, 1);
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("b", "age", 1);
    metaData.setTimeColumn("a", 0);
    metaData.setTimeColumn("b", 1);

    EmbeddingTPGM embedding1 = new EmbeddingTPGM();
    embedding1.add(vertexAId, PropertyValue.create(42));
    embedding1.add(vertexBId, PropertyValue.create(23));
    embedding1.addTimeData(1L, 2L, 3L, 4L);
    embedding1.addTimeData(5L, 6L, 7L, 8L);

    DataSet<EmbeddingTPGM> input = getExecutionEnvironment().fromElements(embedding1);
    PlanNode mockChild = new MockPlanNode(input, metaData);

    List<Pair<String, String>> projectedKeys = new ArrayList<>();
    projectedKeys.add(Pair.of("a", "age"));

    List<EmbeddingTPGM> result =
      new ProjectTemporalEmbeddingsNode(mockChild, projectedKeys).execute().collect();

    Assert.assertThat(result.size(), Is.is(1));
    assertEquals(result.get(0).getId(0), vertexAId);
    assertEquals(result.get(0).getId(1), vertexBId);
    assertThat(result.get(0).getProperties().size(), is(1));
    assertEquals(result.get(0).getProperty(0), PropertyValue.create(42));
    assertArrayEquals(result.get(0).getTimes(0), new Long[] {1L, 2L, 3L, 4L});
    assertArrayEquals(result.get(0).getTimes(1), new Long[] {5L, 6L, 7L, 8L});
    assertEquals(new ProjectTemporalEmbeddingsNode(mockChild, projectedKeys).getEmbeddingMetaData().
      getTimeDataMapping(), metaData.getTimeDataMapping());
    assertEquals(new ProjectTemporalEmbeddingsNode(mockChild, projectedKeys).getEmbeddingMetaData().
      getEntryMapping(), metaData.getEntryMapping());
  }
}
