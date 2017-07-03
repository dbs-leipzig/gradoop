
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Random;

/**
 * Creates a random value for each vertex and filters those that are below a
 * given threshold.
 *
 * @param <V> EPGM vertex type
 */
public class VertexRandomFilter<V extends Vertex>
  implements FilterFunction<V> {
  /**
   * Threshold to decide if a vertex needs to be filtered.
   */
  private final float threshold;
  /**
   * Random instance
   */
  private final Random randomGenerator;

  /**
   * Creates a new filter instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed (can be {@code} null)
   */
  public VertexRandomFilter(float sampleSize, long randomSeed) {
    threshold = sampleSize;
    randomGenerator =
      (randomSeed != 0L) ? new Random(randomSeed) : new Random();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(V vertex) throws Exception {
    return randomGenerator.nextFloat() < threshold;
  }
}
