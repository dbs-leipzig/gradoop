/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.storage.utils;

/**
 * Singleton helper class providing functions for pre-splitting of hbase regions.
 */
public class RegionSplitter {

  /**
   * Default number of regions
   */
  private static final int DEFAULT_REGIONS = 64;

  /**
   * Singleton instance
   */
  private static RegionSplitter INSTANCE = null;

  /**
   * Number of regions, default {@link RegionSplitter#DEFAULT_REGIONS}
   */
  private int numberOfRegions;

  /**
   * The first key of the regions
   */
  private byte[] startKey;

  /**
   * The last key of the regions
   */
  private byte[] endKey;

  /**
   * Creates an instance of this singleton with setting the number of regions to the default
   * value {@link RegionSplitter#DEFAULT_REGIONS}.
   */
  private RegionSplitter() {
    this.numberOfRegions = DEFAULT_REGIONS;
  }

  /**
   * Static function to return the instance of this RegionSplitter.
   *
   * @return the instance of this singleton
   */
  public static RegionSplitter getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new RegionSplitter();
    }
    return INSTANCE;
  }

  /**
   * Returns the start key of the row key range, i.e., 13 Bytes initialized with '0'.
   *
   * @return the start key of the row key range
   */
  public byte[] getStartKey() {
    if (startKey == null) {
      startKey = new byte[13];
    }
    return startKey.clone();
  }

  /**
   * Returns the end key of the row key range, i.e., 13 Bytes where position 0 is initialized
   * with the number of regions and positions 1 - 12 are initialized with '1'.
   *
   * @return the start key of the row key range
   */
  public byte[] getEndKey() {
    if (endKey == null) {
      endKey = new byte[13];

      endKey[0] = (byte) numberOfRegions;

      for (int i = 1; i < 13; i++) {
        endKey[i] = Byte.MAX_VALUE;
      }
    }
    return endKey.clone();
  }

  /**
   * Get the number of regions.
   *
   * @return the number of regions
   */
  public int getNumberOfRegions() {
    return numberOfRegions;
  }

  /**
   * Set the number of regions. The default value is {@link RegionSplitter#DEFAULT_REGIONS}.
   *
   * @param numberOfRegions the number of regions to use
   */
  public void setNumberOfRegions(int numberOfRegions) {
    if (numberOfRegions <= 0) {
      throw new IllegalArgumentException("The number of regions has to be a positive integer.");
    }
    this.numberOfRegions = numberOfRegions;
  }
}
