package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StatisticsTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexCount() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    Long vertexCount = new VertexCount()
      .execute(db)
      .collect()
      .get(0);

    assertThat(vertexCount, is(11L));
  }

  @Test
  public void testEdgeCount() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    Long edgeCount = new EdgeCount()
      .execute(db)
      .collect()
      .get(0);

    assertThat(edgeCount, is(24L));
  }

  @Test
  public void testVertexLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    Map<String, Long> cache = new HashMap<>();

    List<WithCount<String>> result = new VertexLabelDistribution()
      .execute(db)
      .collect();

    assertEquals(3, result.size());

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get("Tag"), is(3L));
    assertThat(cache.get("Forum"), is(2L));
    assertThat(cache.get("Person"), is(6L));
  }

  @Test
  public void testEdgeLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<String>> result = new EdgeLabelDistribution()
      .execute(db)
      .collect();

    assertThat(result.size(), is(5));

    Map<String, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get("hasTag"), is(4L));
    assertThat(cache.get("hasInterest"), is(4L));
    assertThat(cache.get("hasModerator"), is(2L));
    assertThat(cache.get("hasMember"), is(4L));
    assertThat(cache.get("knows"), is(10L));
  }

  @Test
  public void testVertexDegrees() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<GradoopId>> result = new VertexDegrees()
      .execute(db)
      .collect();

    assertThat(result.size(), is(11));

    Map<Long, Integer> dist = new HashMap<>(4);

    result.forEach(e -> dist.put(e.getCount(), dist.getOrDefault(e.getCount(), 0) + 1));

    assertThat(dist.size(), is(4));
    assertThat(dist.get(2L), is(1));
    assertThat(dist.get(3L), is(4));
    assertThat(dist.get(5L), is(2));
    assertThat(dist.get(6L), is(4));
  }

  @Test
  public void testOutgoingVertexDegrees() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<GradoopId>> result = new OutgoingVertexDegrees()
      .execute(db)
      .collect();

    assertThat(result.size(), is(11));

    Map<Long, Integer> dist = new HashMap<>(4);

    result.forEach(e -> dist.put(e.getCount(), dist.getOrDefault(e.getCount(), 0) + 1));

    assertThat(dist.size(), is(4));
    assertThat(dist.get(0L), is(3));
    assertThat(dist.get(2L), is(4));
    assertThat(dist.get(3L), is(2));
    assertThat(dist.get(5L), is(2));
  }

  @Test
  public void testIncomingVertexDegrees() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<GradoopId>> result = new IncomingVertexDegrees()
      .execute(db)
      .collect();

    assertThat(result.size(), is(11));

    Map<Long, Integer> dist = new HashMap<>(4);

    result.forEach(e -> dist.put(e.getCount(), dist.getOrDefault(e.getCount(), 0) + 1));

    assertThat(dist.size(), is(4));
    assertThat(dist.get(0L), is(4));
    assertThat(dist.get(2L), is(1));
    assertThat(dist.get(3L), is(2));
    assertThat(dist.get(4L), is(4));
  }

  @Test
  public void testVertexDegreeDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<Long>> result = new VertexDegreeDistribution()
      .execute(db)
      .collect();

    assertThat(result.size(), is(4));

    Map<Long, Long> cache = new HashMap<>(4);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get(2L), is(1L));
    assertThat(cache.get(3L), is(4L));
    assertThat(cache.get(5L), is(2L));
    assertThat(cache.get(6L), is(4L));
  }

  @Test
  public void testOutgoingVertexDegreeDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<Long>> result = new OutgoingVertexDegreeDistribution()
      .execute(db)
      .collect();

    assertThat(result.size(), is(4));

    Map<Long, Long> cache = new HashMap<>(4);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get(0L), is(3L));
    assertThat(cache.get(2L), is(4L));
    assertThat(cache.get(3L), is(2L));
    assertThat(cache.get(5L), is(2L));
  }

  @Test
  public void testIncomingVertexDegreeDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<Long>> result = new IncomingVertexDegreeDistribution()
      .execute(db)
      .collect();

    assertThat(result.size(), is(4));

    Map<Long, Long> cache = new HashMap<>(4);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get(0L), is(4L));
    assertThat(cache.get(2L), is(1L));
    assertThat(cache.get(3L), is(2L));
    assertThat(cache.get(4L), is(4L));
  }

  @Test
  public void testDistinctSourceIds() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    Long result = new DistinctSourceIds()
      .execute(db)
      .collect()
      .get(0);

    assertThat(result, is(8L));
  }

  @Test
  public void testDistinctTargetIds() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    Long result = new DistinctTargetIds()
      .execute(db)
      .collect()
      .get(0);

    assertThat(result, is(7L));
  }

  @Test
  public void testDistinctSourceIdsByEdgeLabel() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<String>> result = new DistinctSourceIdsByEdgeLabel()
      .execute(db)
      .collect();

    assertThat(result.size(), is(5));

    Map<String, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get("hasInterest"), is(4L));
    assertThat(cache.get("hasModerator"), is(2L));
    assertThat(cache.get("knows"), is(6L));
    assertThat(cache.get("hasTag"), is(2L));
    assertThat(cache.get("hasMember"), is(2L));
  }

  @Test
  public void testDistinctTargetIdsByEdgeLabel() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<String>> result = new DistinctTargetIdsByEdgeLabel()
      .execute(db)
      .collect();

    assertThat(result.size(), is(5));

    Map<String, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get("hasInterest"), is(2L));
    assertThat(cache.get("hasModerator"), is(2L));
    assertThat(cache.get("knows"), is(4L));
    assertThat(cache.get("hasTag"), is(3L));
    assertThat(cache.get("hasMember"), is(4L));
  }

  @Test
  public void testSourceLabelAndEdgeLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<Tuple2<String, String>>> result = new SourceLabelAndEdgeLabelDistribution()
      .execute(db)
      .collect();

    assertThat(result.size(), is(5));

    Map<Tuple2<String, String>, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get(Tuple2.of("Forum","hasModerator")), is(2L));
    assertThat(cache.get(Tuple2.of("Forum","hasTag")), is(4L));
    assertThat(cache.get(Tuple2.of("Person","hasInterest")), is(4L));
    assertThat(cache.get(Tuple2.of("Person","knows")), is(10L));
    assertThat(cache.get(Tuple2.of("Forum","hasMember")), is(4L));
  }

  @Test
  public void testTargetLabelAndEdgeLabelDistribution() throws Exception {
    LogicalGraph db = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    List<WithCount<Tuple2<String, String>>> result = new TargetLabelAndEdgeLabelDistribution()
      .execute(db)
      .collect();

    assertThat(result.size(), is(5));

    Map<Tuple2<String, String>, Long> cache = new HashMap<>(5);

    result.forEach(e -> cache.put(e.getObject(), e.getCount()));

    assertThat(cache.get(Tuple2.of("Tag","hasTag")), is(4L));
    assertThat(cache.get(Tuple2.of("Tag","hasInterest")), is(4L));
    assertThat(cache.get(Tuple2.of("Person","knows")), is(10L));
    assertThat(cache.get(Tuple2.of("Person","hasModerator")), is(2L));
    assertThat(cache.get(Tuple2.of("Person","hasMember")), is(4L));
  }
}
