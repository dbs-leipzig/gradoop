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
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectVerticesNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    String variable = "a";
    FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(
      null, variable, new CNF(), Sets.newHashSet());

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertEquals(0, embeddingMetaData.getEntryColumn(variable));
    assertEquals(0, node.getEmbeddingMetaData().getPropertyKeys(variable).size());
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId vertex1Id = GradoopId.get();
    Map<String, Object> vertex1Props = new HashMap<>();
    vertex1Props.put("foo", 23);

    GradoopId vertex2Id = GradoopId.get();
    Map<String, Object> vertex2Props = new HashMap<>();
    vertex2Props.put("foo", 42);

    EPGMVertex
      vertex1 = new EPGMVertex(vertex1Id, "A", Properties.createFromMap(vertex1Props), new GradoopIdSet());
    EPGMVertex
      vertex2 = new EPGMVertex(vertex2Id, "B", Properties.createFromMap(vertex2Props), new GradoopIdSet());

    DataSet<EPGMVertex> vertices = getExecutionEnvironment().fromElements(vertex1, vertex2);

    String query = "MATCH (n) WHERE n.foo = 23";
    QueryHandler queryHandler = new QueryHandler(query);
    CNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("n"));
    Set<String> projectionKeys = queryHandler.getPredicates().getPropertyKeys("n");

    FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(
      vertices, "n", filterPredicate, projectionKeys);
    List<Embedding> filteredVertices = node.execute().collect();

    assertEquals(1, filteredVertices.size());
    assertEquals(vertex1Id, filteredVertices.get(0).getId(0));
  }
}
