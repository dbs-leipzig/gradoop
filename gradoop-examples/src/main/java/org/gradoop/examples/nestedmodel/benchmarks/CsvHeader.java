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

/**
 * Defines the headeer of the CSV file.
 * To be extended by each row instance
 */
public abstract class CsvHeader {

  /**
   * Header associated to the csv file
   */
  private final String[] header;

  /**
   * Default constructor
   * @param header  Header of the csv file
   */
  public CsvHeader(String... header) {
    this.header = header;
  }

  /**
   * Returns…
   * @return  the header in a CSV format
   */
  public String getHeader() {
    return String.join(",",header);
  }

  /**
   * Converts the object into a csv string
   * @return  CSV representation
   */
  public abstract String valueRowToCSV();

}
