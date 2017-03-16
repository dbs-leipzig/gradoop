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

package org.gradoop.examples.io;

import org.gradoop.examples.io.parsers.amazon.AmazonFileParser;

/**
 * This example shows how to read an Amazon Dataset
 */
public class AmazonExample {

  /**
   * Main program
   * @param args        Default arguments
   */
  public static void main(String[] args) {
    AmazonFileParser afp = new AmazonFileParser();
    afp.fromFile("/Volumes/Untitled/Data/Amazon product data/aggressive_dedup.json");
    afp.asGeneralGraphDataSource(); // clob
  }

}
