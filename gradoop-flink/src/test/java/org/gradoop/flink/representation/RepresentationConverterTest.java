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
package org.gradoop.flink.representation;

import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.representation.transactional.RepresentationConverters;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Test;

import java.util.Set;

public class RepresentationConverterTest extends GradoopFlinkTestBase {

  @Test
  public void testGraphTransactionAdjacencyList() throws Exception {

    GraphTransaction transaction = getGraphTransaction();

    AdjacencyList<GradoopId, String, GradoopId, GradoopId> adjacencyList =
      RepresentationConverters.getAdjacencyList(transaction, new Id<>(), new Id<>());

    GraphTransaction convertedTransaction =
      RepresentationConverters.getGraphTransaction(adjacencyList);

    AdjacencyList<GradoopId, String, GradoopId, GradoopId> convertedAdjacencyList =
      RepresentationConverters
        .getAdjacencyList(convertedTransaction, new Id<>(), new Id<>());

    GradoopFlinkTestUtils.assertEquals(transaction, convertedTransaction);
    GradoopFlinkTestUtils.assertEquals(adjacencyList, convertedAdjacencyList);
  }

  private GraphTransaction getGraphTransaction() {
    EPGMGraphHead graphHead = new EPGMGraphHead(GradoopId.get(), "Test", null);

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());
    Set<EPGMVertex> vertices = Sets.newHashSet();
    Set<EPGMEdge> edges = Sets.newHashSet();

    Properties aProperties = new Properties();
    aProperties.set("x", 1);
    EPGMVertex v1 = new EPGMVertex(GradoopId.get(), "A", aProperties, graphIds);
    EPGMVertex v2 = new EPGMVertex(GradoopId.get(), "B", null, graphIds);

    vertices.add(v1);
    vertices.add(v2);

    Properties loopProperties = new Properties();

    edges.add(new EPGMEdge(GradoopId.get(), "loop", v1.getId(), v1.getId(), loopProperties, graphIds));
    edges.add(new EPGMEdge(GradoopId.get(), "m", v1.getId(), v2.getId(), null, graphIds));
    edges.add(new EPGMEdge(GradoopId.get(), "m", v1.getId(), v2.getId(), null, graphIds));
    edges.add(new EPGMEdge(GradoopId.get(), "m", v2.getId(), v1.getId(), null, graphIds));

    return new GraphTransaction(graphHead, vertices, edges);
  }
}
