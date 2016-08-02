package org.gradoop.flink.model.impl.id;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FlinkGradoopIdSetTest extends GradoopFlinkTestBase {

  @Test
  public void testEquals() throws Exception {

    int idCount = 100;
    List<GradoopId> ids = Lists.newArrayListWithCapacity(idCount);

    for(int i = 0; i < idCount; i++) {
      ids.add(GradoopId.get());
    }

    GradoopIdSet set1 = GradoopIdSet.fromExisting(
      ids.toArray(new GradoopId[idCount]));

    GradoopIdSet set2 = GradoopIdSet.fromExisting(
      ids.toArray(new GradoopId[idCount]));

    Collections.shuffle(ids);

    GradoopIdSet set3 = GradoopIdSet.fromExisting(
      ids.toArray(new GradoopId[idCount]));

    ExecutionEnvironment env = getExecutionEnvironment();

    DataSet<Tuple1<GradoopIdSet>> ds1 = env
      .fromElements(new Tuple1<>(set1));

    DataSet<Tuple1<GradoopIdSet>> ds2 = env
      .fromElements(new Tuple1<>(set2));

    DataSet<Tuple1<GradoopIdSet>> ds3 = env
      .fromElements(new Tuple1<>(set3));

    assertCount("self join", ds1, ds1, 1L);
    assertCount("same elements and same order join", ds1, ds2, 1L);
    assertCount("same elements shuffled join", ds1, ds3, 1L);
  }

  private <T> void assertCount(
    String message,
    DataSet<Tuple1<T>> ds1,
    DataSet<Tuple1<T>> ds2,
    Long expectedCount
  ) throws Exception {

    List<Long> joinCountTupleList = Count.count(ds1
      .join(ds2)
      .where(0).equalTo(0)
    )
    .collect();

    Long joinCount = joinCountTupleList.isEmpty() ?
      0 :
      joinCountTupleList.get(0);

    assertEquals(message, joinCount, expectedCount);
  }

}
