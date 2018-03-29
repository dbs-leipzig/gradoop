package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Direction;

import java.util.ArrayList;
import java.util.List;

/**
 * COPIED WITH SMALL MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * Emits the two-path for all neighbor pairs in this group.
 * <p>
 * The first {@link #groupSize} vertices are emitted pairwise. Following
 * vertices are only paired with vertices from this initial group.
 *
 * @see GenerateGroupSpans
 */
public class GenerateGroupPairs implements
  GroupReduceFunction<Tuple4<IntValue, GradoopId, GradoopId, IntValue>, Tuple3<GradoopId,
    GradoopId, IntValue>> {
  private final int groupSize;
  private Direction direction;

  private boolean initialized = false;

  private List<Tuple3<GradoopId, GradoopId, IntValue>> visited;

  public GenerateGroupPairs(int groupSize, Direction direction) {
    this.groupSize = groupSize;
    this.direction = direction;
    this.visited = new ArrayList<>(groupSize);
  }

  @Override
  public void reduce(Iterable<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> values,
    Collector<Tuple3<GradoopId, GradoopId, IntValue>> out) throws Exception {
    int visitedCount = 0;
    for (Tuple4<IntValue, GradoopId, GradoopId, IntValue> edge : values) {
      for (int i = 0; i < visitedCount; i++) {
        Tuple3<GradoopId, GradoopId, IntValue> prior = visited.get(i);

        prior.f1 = direction.equals(Direction.OUTDEGREE) ? edge.f1 : edge.f2;

        int oldValue = prior.f2.getValue();

        // TODO: statt sum kann hier max oder so rein
        long degreeSum = oldValue + edge.f3.getValue();
        if (degreeSum > Integer.MAX_VALUE) {
          throw new RuntimeException("Degree sum overflows IntValue");
        }
        prior.f2.setValue((int) degreeSum);

        // v, w, d(v) + d(w)
        out.collect(prior);

        prior.f2.setValue(oldValue);
      }

      if (visitedCount < groupSize) {
        if (!initialized) {
          initialized = true;

          for (int i = 0; i < groupSize; i++) {
            Tuple3<GradoopId, GradoopId, IntValue> tuple = new Tuple3<>();

            tuple.f0 = direction.equals(Direction.OUTDEGREE) ? edge.f1.copy() : edge.f2.copy();
            tuple.f2 = edge.f3.copy();

            visited.add(tuple);
          }
        } else {
          Tuple3<GradoopId, GradoopId, IntValue> copy = visited.get(visitedCount);

          edge.f2.copyTo(copy.f0);
          edge.f3.copyTo(copy.f2);
        }

        visitedCount += 1;
      }
    }
  }
}