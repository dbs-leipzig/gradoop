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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Singleton helper class providing functions to salt HBase row keys with a prefix.
 *
 * This implementation reuses code of <a href="https://github.com/sematext/HBaseWD">HBaseWD</a>
 * (com.sematext.hbase.wd.RowKeyDistributorByOneBytePrefix). Some code is copied directly or has
 * only small changes.
 */
public class RowKeyDistributor {

  /**
   * Default number of buckets (which is the max prefix byte)
   */
  private static final byte DEFAULT_MAX_PREFIX = (byte) 64;

  /**
   * Singleton instance
   */
  private static RowKeyDistributor INSTANCE = null;

  /**
   * Internal array of prefixes, initialized in the constructor.
   */
  private final byte[][] prefixes;

  /**
   * The maximum prefix
   */
  private byte maxPrefix;

  /**
   * The next prefix byte (initialized with 0)
   */
  private byte nextPrefix;

  /**
   * Creates an instance of this singleton with setting the max prefix to the default
   * value {@link RowKeyDistributor#DEFAULT_MAX_PREFIX}.
   */
  private RowKeyDistributor() {
    this.maxPrefix = DEFAULT_MAX_PREFIX;
    this.prefixes = new byte[Byte.MAX_VALUE][];
    for (byte i = 0; i < Byte.MAX_VALUE; i++) {
      prefixes[i] = new byte[] {i};
    }
  }

  /**
   * Static function to return the instance of this RowKeyDistributor.
   *
   * @return the instance of this singleton
   */
  public static RowKeyDistributor getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new RowKeyDistributor();
    }
    return INSTANCE;
  }

  /**
   * Get a row key that is salted with a 1 Byte prefix. The prefix is a random Byte between 0 and
   * maxPrefix.
   *
   * @param originalKey the original key to add the prefix
   * @return the original key with a prefix byte on the first position
   */
  public byte[] getDistributedKey(byte[] originalKey) {
    byte[] key = Bytes.add(prefixes[nextPrefix++], originalKey);
    nextPrefix = (byte) (nextPrefix % maxPrefix);

    return key;
  }

  /**
   * Get the original key by the given key with a prefix.
   *
   * @param adjustedKey the key with a prefix
   * @return the original key without the prefix
   */
  public byte[] getOriginalKey(byte[] adjustedKey) {
    return Bytes.tail(adjustedKey, adjustedKey.length - 1);
  }

  /**
   * Get an array of all possible keys for a given original key.
   *
   * @param originalKey the original key
   * @return an array of size maxPrefix with all possible keys
   */
  public byte[][] getAllDistributedKeys(byte[] originalKey) {
    byte[][] keys = new byte[maxPrefix][];
    for (byte i = 0; i < maxPrefix; i++) {
      keys[i] = Bytes.add(prefixes[i], originalKey);
    }
    return keys;
  }

  /**
   * Set the bucket count manually. It is used as the maximal prefix.
   *
   * @param bucketCount the bucket count as byte
   */
  public void setBucketCount(byte bucketCount) {
    this.maxPrefix = bucketCount;
  }
}
