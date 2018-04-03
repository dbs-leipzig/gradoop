package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType;

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
  private NeighborhoodType neighborhoodType;
  private Denominator denominator;
  private boolean initialized = false;

  private List<Tuple3<GradoopId, GradoopId, IntValue>> visited;

  public GenerateGroupPairs(int groupSize, NeighborhoodType neighborhoodType, Denominator
    denominator) {
    this.groupSize = groupSize;
    this.neighborhoodType = neighborhoodType;
    this.denominator = denominator;
    this.visited = new ArrayList<>(groupSize);
  }

  @Override
  public void reduce(Iterable<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> values,
    Collector<Tuple3<GradoopId, GradoopId, IntValue>> out) throws Exception {
    int visitedCount = 0;
    for (Tuple4<IntValue, GradoopId, GradoopId, IntValue> edge : values) {
      for (int i = 0; i < visitedCount; i++) {
        Tuple3<GradoopId, GradoopId, IntValue> prior = visited.get(i);

        prior.f1 = neighborhoodType.equals(NeighborhoodType.OUT) ? edge.f1 : edge.f2;

        int oldValue = prior.f2.getValue();

        long degreeSum = getDegreeCombination(oldValue,edge.f3.getValue());
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

            tuple.f0 = neighborhoodType.equals(NeighborhoodType.OUT) ? edge.f1.copy() : edge.f2.copy();
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

  /**
   * Returns the combination of th given vertex degrees in dependency of the chosen
   * {@link Denominator}
   * @param oldValue
   * @param value
   * @return
   */
  private long getDegreeCombination(int oldValue, int value) {
    if(denominator.equals(Denominator.UNION)) {
      return oldValue + value;
    } else {
      return Math.max(oldValue, value);
    }
  }
}