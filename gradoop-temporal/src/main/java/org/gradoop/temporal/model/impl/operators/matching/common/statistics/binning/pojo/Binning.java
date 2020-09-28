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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Implements equal frequency binning of Long values.
 */
public class Binning implements Serializable {

  /**
   * Default number of bins
   */
  public static final int DEFAULT_NUM_OF_BINS = 100;
  /**
   * represents the bins. Every bin is defined by its lowest value
   */
  private Long[] bins = new Long[] {};

  /**
   * Creates new bins from values. Furthermore, the number of bins to create is given.
   *
   * @param values       values to create bins from
   * @param numberOfBins number of bins to create
   */
  public Binning(List<Long> values, int numberOfBins) {
    createBins(values, numberOfBins);
  }

  /**
   * Creates new bins from values. Number of bins is default.
   *
   * @param values values to create bins from
   */
  public Binning(List<Long> values) {
    this(values, DEFAULT_NUM_OF_BINS);
  }

  /**
   * Creates the bins from values. The value list is sorted and divided into
   * equal sized bins. The first bin gets lowest value {@code Long.MIN_VALUE},
   * every other bin's lowest value is computed as
   * {@code max(previous bin) + (min(current bin) - max(previous bin))/2}
   *
   * @param values       values to create bins from. Its size must be a multiple of the specified
   *                     number of bins!
   * @param numberOfBins number of bins to create
   * @throws IllegalArgumentException if number ov values is not a multiple of the number of bins
   */
  private void createBins(List<Long> values, int numberOfBins) throws IllegalArgumentException {
    if (values.size() % numberOfBins != 0) {
      throw new IllegalArgumentException("Number of values must be a multiple " +
        "of the number of bins!");
    }

    values.sort((aLong, t1) -> Long.compare(aLong, t1));

    // there can not be more bins than values
    if (values.size() < numberOfBins) {
      numberOfBins = values.size();
    }

    // size of a bin is the number of elements contained
    int binSize = (int) Math.ceil((double) values.size() / (double) numberOfBins);


    bins = new Long[numberOfBins];
    bins[0] = Long.MIN_VALUE;

    for (int i = 1; i < numberOfBins; i++) {
      long maxPreviousBin = values.get((i * binSize) - 1);
      long minCurrentBin = values.get(i * binSize);
      bins[i] = maxPreviousBin + (minCurrentBin - maxPreviousBin) / 2;
    }


  }

  /**
   * Returns the bins
   *
   * @return bins
   */
  public Long[] getBins() {
    return bins.clone();
  }


  public void setBins(Long[] bins) {
    this.bins = bins.clone();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bins);
  }
}
