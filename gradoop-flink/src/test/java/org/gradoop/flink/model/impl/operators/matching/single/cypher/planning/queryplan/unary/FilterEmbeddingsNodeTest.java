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

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData.EntryType;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FilterEmbeddingsNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingMetaData mockMeta = new EmbeddingMetaData();
    mockMeta.setEntryColumn("a", EntryType.VERTEX, 0);
    mockMeta.setPropertyColumn("a", "age", 0);
    PlanNode mockNode = new MockPlanNode(null, mockMeta);

    FilterEmbeddingsNode node = new FilterEmbeddingsNode(mockNode, new CNF());

    assertTrue(mockNode.getEmbeddingMetaData().equals(node.getEmbeddingMetaData()));
  }

  @Test
  public void testExecute() throws Exception {
    String query = "MATCH (a)-->(b) WHERE a.age > b.age";
    QueryHandler queryHandler = new QueryHandler(query);
    CNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("a", "b"));

    GradoopId vertexAId = GradoopId.get();
    GradoopId vertexBId = GradoopId.get();
    GradoopId vertexCId = GradoopId.get();

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EntryType.VERTEX, 0);
    metaData.setEntryColumn("b", EntryType.VERTEX, 1);
    metaData.setPropertyColumn("a", "age", 0);
    metaData.setPropertyColumn("b", "age", 1);

    Embedding embedding1 = new Embedding();
    embedding1.add(vertexAId, PropertyValue.create(42));
    embedding1.add(vertexBId, PropertyValue.create(23));

    Embedding embedding2 = new Embedding();
    embedding2.add(vertexAId, PropertyValue.create(42));
    embedding2.add(vertexCId, PropertyValue.create(84));

    DataSet<Embedding> input = getExecutionEnvironment().fromElements(embedding1, embedding2);
    PlanNode mockChild = new MockPlanNode(input, metaData);

    List<Embedding> result = new FilterEmbeddingsNode(mockChild, filterPredicate).execute().collect();

    assertThat(result.size(), is(1));
    assertTrue(result.get(0).getId(0).equals(vertexAId));
    assertTrue(result.get(0).getId(1).equals(vertexBId));
    assertTrue(result.get(0).getProperty(0).equals(PropertyValue.create(42)));
    assertTrue(result.get(0).getProperty(1).equals(PropertyValue.create(23)));
  }
}
