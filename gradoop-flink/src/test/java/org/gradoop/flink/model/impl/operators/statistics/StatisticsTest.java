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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StatisticsTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexCount() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    Long vertexCount = new VertexCount()
      .execute(db)
      .collect()
      .get(0);

    assertEquals(11L, (long) vertexCount);
  }

  @Test
  public void testEdgeCount() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    Long edgeCount = new EdgeCount()
      .execute(db)
      .collect()
      .get(0);

    assertEquals(24L, (long) edgeCount);
  }

  @Test
  public void testVertexLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    Map<String, Long> cache = new HashMap<>();

    List<WithCount<String>> result = new VertexLabelDistribution()
      .execute(db)
      .collect();

    assertEquals(3, result.size());

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(3L, (long) cache.get("Tag"));
    assertEquals(2L, (long) cache.get("Forum"));
    assertEquals(6L, (long) cache.get("Person"));
  }

  @Test
  public void testEdgeLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<String>> result = new EdgeLabelDistribution()
      .execute(db)
      .collect();

    assertEquals(5, result.size());

    Map<String, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(4L, (long) cache.get("hasTag"));
    assertEquals(4L, (long) cache.get("hasInterest"));
    assertEquals(2L, (long) cache.get("hasModerator"));
    assertEquals(4L, (long) cache.get("hasMember"));
    assertEquals(10L, (long) cache.get("knows"));
  }

  @Test
  public void testVertexDegrees() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<GradoopId>> result = new VertexDegrees()
      .execute(db)
      .collect();

    assertEquals(11, result.size());

    Map<Long, Integer> dist = new HashMap<>(4);

    result.forEach(e -> dist.put(e.getCount(), dist.getOrDefault(e.getCount(), 0) + 1));

    assertEquals(4, dist.size());
    assertEquals(1, (int) dist.get(2L));
    assertEquals(4, (int) dist.get(3L));
    assertEquals(2, (int) dist.get(5L));
    assertEquals(4, (int) dist.get(6L));
  }

  @Test
  public void testOutgoingVertexDegrees() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<GradoopId>> result = new OutgoingVertexDegrees()
      .execute(db)
      .collect();

    assertEquals(11, result.size());

    Map<Long, Integer> dist = new HashMap<>(4);

    result.forEach(e -> dist.put(e.getCount(), dist.getOrDefault(e.getCount(), 0) + 1));

    assertEquals(4, dist.size());
    assertEquals(3, (int) dist.get(0L));
    assertEquals(4, (int) dist.get(2L));
    assertEquals(2, (int) dist.get(3L));
    assertEquals(2, (int) dist.get(5L));
  }

  @Test
  public void testIncomingVertexDegrees() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<GradoopId>> result = new IncomingVertexDegrees()
      .execute(db)
      .collect();

    assertEquals(11, result.size());

    Map<Long, Integer> dist = new HashMap<>(4);

    result.forEach(e -> dist.put(e.getCount(), dist.getOrDefault(e.getCount(), 0) + 1));

    assertEquals(4, dist.size());
    assertEquals(4, (int) dist.get(0L));
    assertEquals(1, (int) dist.get(2L));
    assertEquals(2, (int) dist.get(3L));
    assertEquals(4, (int) dist.get(4L));
  }

  @Test
  public void testVertexDegreeDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<Long>> result = new VertexDegreeDistribution()
      .execute(db)
      .collect();

    assertEquals(4, result.size());

    Map<Long, Long> cache = new HashMap<>(4);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(1L, (long) cache.get(2L));
    assertEquals(4L, (long) cache.get(3L));
    assertEquals(2L, (long) cache.get(5L));
    assertEquals(4L, (long) cache.get(6L));
  }

  @Test
  public void testOutgoingVertexDegreeDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<Long>> result = new OutgoingVertexDegreeDistribution()
      .execute(db)
      .collect();

    assertEquals(4, result.size());

    Map<Long, Long> cache = new HashMap<>(4);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(3L, (long) cache.get(0L));
    assertEquals(4L, (long) cache.get(2L));
    assertEquals(2L, (long) cache.get(3L));
    assertEquals(2L, (long) cache.get(5L));
  }

  @Test
  public void testIncomingVertexDegreeDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<Long>> result = new IncomingVertexDegreeDistribution()
      .execute(db)
      .collect();

    assertEquals(4, result.size());

    Map<Long, Long> cache = new HashMap<>(4);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(4L, (long) cache.get(0L));
    assertEquals(1L, (long) cache.get(2L));
    assertEquals(2L, (long) cache.get(3L));
    assertEquals(4L, (long) cache.get(4L));
  }

  @Test
  public void testDistinctSourceIds() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    Long result = new DistinctSourceIds()
      .execute(db)
      .collect()
      .get(0);

    assertEquals(8L, (long) result);
  }

  @Test
  public void testDistinctTargetIds() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    Long result = new DistinctTargetIds()
      .execute(db)
      .collect()
      .get(0);

    assertEquals(7L, (long) result);
  }

  @Test
  public void testDistinctSourceIdsByEdgeLabel() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<String>> result = new DistinctSourceIdsByEdgeLabel()
      .execute(db)
      .collect();

    assertEquals(5, result.size());

    Map<String, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(4L, (long) cache.get("hasInterest"));
    assertEquals(2L, (long) cache.get("hasModerator"));
    assertEquals(6L, (long) cache.get("knows"));
    assertEquals(2L, (long) cache.get("hasTag"));
    assertEquals(2L, (long) cache.get("hasMember"));
  }

  @Test
  public void testDistinctTargetIdsByEdgeLabel() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<String>> result = new DistinctTargetIdsByEdgeLabel()
      .execute(db)
      .collect();

    assertEquals(5, result.size());

    Map<String, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(2L, (long) cache.get("hasInterest"));
    assertEquals(2L, (long) cache.get("hasModerator"));
    assertEquals(4L, (long) cache.get("knows"));
    assertEquals(3L, (long) cache.get("hasTag"));
    assertEquals(4L, (long) cache.get("hasMember"));
  }

  @Test
  public void testSourceLabelAndEdgeLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<Tuple2<String, String>>> result = new SourceLabelAndEdgeLabelDistribution()
      .execute(db)
      .collect();

    assertEquals(5, result.size());

    Map<Tuple2<String, String>, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(2L, (long) cache.get(Tuple2.of("Forum", "hasModerator")));
    assertEquals(4L, (long) cache.get(Tuple2.of("Forum", "hasTag")));
    assertEquals(4L, (long) cache.get(Tuple2.of("Person", "hasInterest")));
    assertEquals(10L, (long) cache.get(Tuple2.of("Person", "knows")));
    assertEquals(4L, (long) cache.get(Tuple2.of("Forum", "hasMember")));
  }

  @Test
  public void testTargetLabelAndEdgeLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<Tuple2<String, String>>> result = new TargetLabelAndEdgeLabelDistribution()
      .execute(db)
      .collect();

    assertEquals(5, result.size());

    Map<Tuple2<String, String>, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(4L, (long) cache.get(Tuple2.of("Tag", "hasTag")));
    assertEquals(4L, (long) cache.get(Tuple2.of("Tag", "hasInterest")));
    assertEquals(10L, (long) cache.get(Tuple2.of("Person", "knows")));
    assertEquals(2L, (long) cache.get(Tuple2.of("Person", "hasModerator")));
    assertEquals(4L, (long) cache.get(Tuple2.of("Person", "hasMember")));
  }


  @Test
  public void testDistinctEdgePropertyValuesByLabelAndPropertyName() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<Tuple2<String, String>>> result =
      new DistinctEdgePropertiesByLabel()
        .execute(db)
        .collect();

    Map<Tuple, Long> cache = new HashMap<>(5);
    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(2, result.size());
    assertEquals(3L, (long) cache.get(Tuple2.of("knows", "since")));
    assertEquals(1L, (long) cache.get(Tuple2.of("hasModerator", "since")));
  }

  @Test
  public void testDistinctVertexPropertyValuesByLabelAndPropertyName() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<Tuple2<String, String>>> result =
      new DistinctVertexPropertiesByLabel()
        .execute(db)
        .collect();

    Map<Tuple, Long> cache = new HashMap<>(5);
    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(8, result.size());

    assertEquals(6L, (long) cache.get(Tuple2.of("Person", "name")));
    assertEquals(2L, (long) cache.get(Tuple2.of("Person", "gender")));
    assertEquals(3L, (long) cache.get(Tuple2.of("Person", "city")));
    assertEquals(4L, (long) cache.get(Tuple2.of("Person", "age")));
    assertEquals(1L, (long) cache.get(Tuple2.of("Person", "speaks")));
    assertEquals(1L, (long) cache.get(Tuple2.of("Person", "locIP")));
    assertEquals(3L, (long) cache.get(Tuple2.of("Tag", "name")));
    assertEquals(2L, (long) cache.get(Tuple2.of("Forum", "title")));
  }

  @Test
  public void testDistinctEdgePropertyValuesByPropertyName() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<String>> result =
      new DistinctEdgeProperties()
        .execute(db)
        .collect();

    Map<String, Long> cache = new HashMap<>(5);
    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(1, result.size());
    assertEquals(3L, (long) cache.get("since"));
  }

  @Test
  public void testDistinctVertexPropertyValuesByPropertyName() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getLogicalGraph();

    List<WithCount<String>> result =
      new DistinctVertexProperties()
        .execute(db)
        .collect();

    Map<String, Long> cache = new HashMap<>(5);
    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertEquals(7, result.size());

    assertEquals(9L, (long) cache.get("name"));
    assertEquals(2L, (long) cache.get("gender"));
    assertEquals(3L, (long) cache.get("city"));
    assertEquals(4L, (long) cache.get("age"));
    assertEquals(1L, (long) cache.get("speaks"));
    assertEquals(1L, (long) cache.get("locIP"));
    assertEquals(2L, (long) cache.get("title"));
  }
}
