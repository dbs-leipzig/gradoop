/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
