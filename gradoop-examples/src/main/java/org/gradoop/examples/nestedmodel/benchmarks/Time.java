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

package org.gradoop.examples.nestedmodel.benchmarks;

import java.util.concurrent.TimeUnit;

/**
 * Typing time expression
 */
public class Time {

  /**
   * Time unit in which the benchmarks are expressed
   */
  private final TimeUnit unit;

  /**
   * Time value
   */
  private final long time;

  /**
   * Default constructor
   * @param unit  measure time
   * @param time  time
   */
  public Time(TimeUnit unit, long time) {
    this.unit = unit;
    this.time = time;
  }

  /**
   * Returns…
   * @return  the current system's millisecond time
   */
  public static Time milliseconds() {
    return new Time(TimeUnit.MILLISECONDS, System.currentTimeMillis());
  }

  /**
   * Returns…
   * @return  the current systems' nanosecond time
   */
  public static Time nanoseconds() {
    return new Time(TimeUnit.NANOSECONDS, System.nanoTime());
  }

  /**
   * Returns the smallest representation unit between two given Times
   * @param left    Left operand
   * @param right   Right operand
   * @return        Minimum between the two
   */
  public static TimeUnit smallestRepresentation(TimeUnit left, TimeUnit right) {
    switch (left) {
    case NANOSECONDS:
      return right;
    case MICROSECONDS:
      return right.equals(TimeUnit.NANOSECONDS) ? right : left;
    case MILLISECONDS:
      return right.equals(TimeUnit.NANOSECONDS) || right.equals(TimeUnit.MICROSECONDS) ?
              right :
              left;
    case SECONDS:
      return (right.equals(TimeUnit.NANOSECONDS)  ||
               right.equals(TimeUnit.MICROSECONDS) ||
               right.equals(TimeUnit.MILLISECONDS)   ) ?
        right :
        left;
    case MINUTES:
      return right.equals(TimeUnit.HOURS) || right.equals(TimeUnit.DAYS) ?
        left :
        right;
    case HOURS:
      return right.equals(TimeUnit.DAYS) ? left : right;
    case DAYS:
      return left;
    default:
      return right;
    }
  }

  /**
   * Converts the current time representation to a given time unit
   * @param tu    New time unit
   * @return      Converted time
   */
  public Time convertTo(TimeUnit tu) {
    return new Time(tu, unit.convert(time, tu));
  }

  /**
   * Performs the difference between two elements. The current element acts as the
   * left operand.
   * @param right   Right operand
   * @return        Time difference represented using the smallest representation
   *                between the two.
   */
  public Time difference(Time right) {
    TimeUnit smallest = smallestRepresentation(unit, right.unit);
    Time thisTime = convertTo(smallest);
    Time rightTime = right.convertTo(smallest);
    return new Time(smallest, thisTime.time - rightTime.time);
  }

  /**
   * Returns…
   * @return  the time representation that has been choosen
   */
  public TimeUnit getRepresentation() {
    return unit;
  }

  /**
   * Returns…
   * @return  the actual time value in a given representation
   */
  public long getTime() {
    return time;
  }
}
