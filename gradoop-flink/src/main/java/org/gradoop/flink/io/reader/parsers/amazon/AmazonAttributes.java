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

package org.gradoop.flink.io.reader.parsers.amazon;

/**
 * Enumerating all the possible attributes
 */
public enum AmazonAttributes {
  /**
   * Reviewer id
   */
  reviewerID("reviewerID"),

  /**
   * Product id
   */
  asin("asin"),

  /**
   * Name associated to the reviewer
   */
  reviewerName("reviewerName"),

  /**
   * Helpfullness expressed as a fraction get(0)/get(1)
   */
  helpful("helpful"),

  /**
   * The actual review
   */
  reviewText("reviewText"),

  /**
   * Defines the actual rating
   */
  overall("overall"),

  /**
   * String brief summary
   */
  summary("summary"),

  /**
   * Machine-readable time
   */
  unixReviewTime("unixReviewTime"),

  /**
   * Human readable review time
   */
  reviewTime("reviewTime");

  /**
   * String value
   */
  private String val;

  /**
   * Default constructor
   * @param reviewerName string associated to the constant
   */
  AmazonAttributes(String reviewerName) {
    this.val = reviewerName;
  }

  /**
   * Returns…
   * @return  the default value
   */
  public String value() {
    return val;
  }

}
