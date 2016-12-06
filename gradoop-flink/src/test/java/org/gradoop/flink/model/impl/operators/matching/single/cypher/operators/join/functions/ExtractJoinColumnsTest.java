package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class ExtractJoinColumnsTest extends PhysicalOperatorTest {

  @Test
  public void testSingleColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    Embedding embedding = createEmbedding(v0, v1);

    ExtractJoinColumns udf = new ExtractJoinColumns(Collections.singletonList(0));

    Assert.assertEquals(v0.toString(), udf.getKey(embedding));
  }

  @Test
  public void testMultiColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    Embedding embedding = createEmbedding(v0, v1);

    ExtractJoinColumns udf = new ExtractJoinColumns(Arrays.asList(0, 1));

    Assert.assertEquals(v0.toString() + v1.toString(), udf.getKey(embedding));
  }

  @Test
  public void testMultiColumnReverse() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    Embedding embedding = createEmbedding(v0, v1);

    ExtractJoinColumns udf = new ExtractJoinColumns(Arrays.asList(1, 0));

    Assert.assertEquals(v1.toString() + v0.toString(), udf.getKey(embedding));
  }
}
