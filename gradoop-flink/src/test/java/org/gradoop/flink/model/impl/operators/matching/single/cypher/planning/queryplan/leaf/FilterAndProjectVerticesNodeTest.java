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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FilterAndProjectVerticesNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    String variable = "a";
    FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(
      null, variable, new CNF(), Sets.newHashSet());

    EmbeddingMetaData embeddingMetaData = node.getEmbeddingMetaData();
    assertThat(embeddingMetaData.getEntryColumn(variable), is(0));
    assertThat(node.getEmbeddingMetaData().getPropertyKeys(variable).size(), is(0));
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId vertex1Id = GradoopId.get();
    Map<String, Object> vertex1Props = new HashMap<>();
    vertex1Props.put("foo", 23);

    GradoopId vertex2Id = GradoopId.get();
    Map<String, Object> vertex2Props = new HashMap<>();
    vertex2Props.put("foo", 42);

    Vertex vertex1 = new Vertex(vertex1Id, "A", Properties.createFromMap(vertex1Props), new GradoopIdSet());
    Vertex vertex2 = new Vertex(vertex2Id, "B", Properties.createFromMap(vertex2Props), new GradoopIdSet());

    DataSet<Vertex> vertices = getExecutionEnvironment().fromElements(vertex1, vertex2);

    String query = "MATCH (n) WHERE n.foo = 23";
    QueryHandler queryHandler = new QueryHandler(query);
    CNF filterPredicate = queryHandler.getPredicates().getSubCNF(Sets.newHashSet("n"));
    Set<String> projectionKeys = queryHandler.getPredicates().getPropertyKeys("n");

    FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(
      vertices, "n", filterPredicate, projectionKeys);
    List<Embedding> filteredVertices = node.execute().collect();

    assertThat(filteredVertices.size(), is(1));
    assertThat(filteredVertices.get(0).getId(0).equals(vertex1Id), is(true));
  }
}
