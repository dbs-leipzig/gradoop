package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang.ArrayUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.createEmbedding;

public class ExtractJoinColumnsTest extends PhysicalOperatorTest {

  @Test
  public void testSingleColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    Embedding embedding = createEmbedding(v0, v1);

    ExtractJoinColumns udf = new ExtractJoinColumns(Collections.singletonList(0));

    Assert.assertEquals(ArrayUtils.toString(v0.toByteArray()), udf.getKey(embedding));
  }

  @Test
  public void testMultiColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    Embedding embedding = createEmbedding(v0, v1);

    ExtractJoinColumns udf = new ExtractJoinColumns(Arrays.asList(0, 1));

    Assert.assertEquals(
      ArrayUtils.toString(v0.toByteArray()) + ArrayUtils.toString(v1.toByteArray()),
      udf.getKey(embedding)
    );
  }

  @Test
  public void testMultiColumnReverse() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    Embedding embedding = createEmbedding(v0, v1);

    ExtractJoinColumns udf1 = new ExtractJoinColumns(Arrays.asList(0, 1));
    ExtractJoinColumns udf2 = new ExtractJoinColumns(Arrays.asList(1, 0));

    Assert.assertNotEquals(udf1.getKey(embedding), udf2.getKey(embedding));
  }
}
