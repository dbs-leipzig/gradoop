/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.common.util;

/**
 * Contains math helper methods.
 */
public abstract class MathHelper {
  /**
   * Pairing function to uniquely encode two natural numbers into a single
   * number.
   *
   * @param x first number (>= 0)
   * @param y second number (>= 0)
   * @return uniquely encoded natural number
   */
  public static long cantor(long x, long y) {
    return (x + y + 1) * (x + y) / 2 + y;
  }

  /**
   * Creates number pair from given cantor number.
   *
   * @param z cantor number
   * @return first and second number
   */
  public static long[] reverseCantor(long z) {
    long[] pair = new long[2];
    long t = (long) (Math.floor(-1d + Math.sqrt(1d + 8 * z) / 2d));
    pair[0] = t * (t + 3) / 2 - z;
    pair[1] = z - t * (t + 1) / 2;
    return pair;
  }
}
