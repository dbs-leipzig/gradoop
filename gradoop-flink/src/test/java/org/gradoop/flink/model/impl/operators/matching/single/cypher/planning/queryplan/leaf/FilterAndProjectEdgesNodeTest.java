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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectEdgesNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() {
    String sourceVariable = "a";
    String edgeVariable   = "e";
    String targetVariable = "b";
    FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode<>(
      null, sourceVariable, edgeVariable, targetVariable, new CNF(), new HashSet<>(), false);

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertEquals(0, embeddingMetaData.getEntryColumn(sourceVariable));
    assertEquals(1, embeddingMetaData.getEntryColumn(edgeVariable));
    assertEquals(2, embeddingMetaData.getEntryColumn(targetVariable));
    assertEquals(0, embeddingMetaData.getPropertyKeys(edgeVariable).size());
  }

  @Test
  public void testMetaDataInitializationWithLoop() {
    String sourceVariable = "a";
    String edgeVariable   = "e";
    String targetVariable = "a";
    FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode<>(
      null, sourceVariable, edgeVariable, targetVariable, new CNF(), new HashSet<>(), false);

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertEquals(0, embeddingMetaData.getEntryColumn(sourceVariable));
    assertEquals(1, embeddingMetaData.getEntryColumn(edgeVariable));
    assertEquals(0, embeddingMetaData.getEntryColumn(targetVariable));
    assertEquals(0, embeddingMetaData.getPropertyKeys(edgeVariable).size());
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    GradoopId edge1Id = GradoopId.get();
    Map<String, Object> edge1Props = new HashMap<>();
    edge1Props.put("foo", 23);

    GradoopId edge2Id = GradoopId.get();
    Map<String, Object> edge2Props = new HashMap<>();
    edge2Props.put("foo", 42);

    EPGMEdge e1 = new EPGMEdge(edge1Id, "a", sourceId, targetId, Properties.createFromMap(edge1Props),
      new GradoopIdSet());
    EPGMEdge e2 = new EPGMEdge(edge2Id, "b", sourceId, targetId, Properties.createFromMap(edge2Props),
      new GradoopIdSet());

    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(e1, e2);

    String query = "MATCH (a)-[e]->(b) WHERE e.foo = 23";
    QueryHandler queryHandler = new QueryHandler(query);
    CNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("e"));
    Set<String> projectionKeys = queryHandler.getPredicates().getPropertyKeys("e");

    FilterAndProjectEdgesNode<EPGMEdge> node = new FilterAndProjectEdgesNode<>(edges, "a", "e", "b",
      filterPredicate, projectionKeys, false);

    List<Embedding> filteredEdges = node.execute().collect();

    assertEquals(1, filteredEdges.size());
    assertEquals(sourceId, filteredEdges.get(0).getId(0));
    assertEquals(edge1Id, filteredEdges.get(0).getId(1));
    assertEquals(targetId, filteredEdges.get(0).getId(2));
  }
}
