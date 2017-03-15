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
 * Defines the row for the benchmark results for the Nesting operator
 */
public class BenchmarkResult extends CsvHeader {

  /**
   * Dataset size
   */
  private long datasetSize;

  /**
   * Number of nested graph in the operand
   */
  private long nestingOperandNumberOfElements;

  /**
   * number of elements for each operands' elements
   */
  private long nestingOperandSizeForEachElement;

  /**
   * the current running time
   */
  private Time unit;

  /**
   * Default constructor
   */
  public BenchmarkResult() {
    super("datasetSize", "nestingOperandNumberOfElements",
      "nestingOperandSizeForEachElement", "Representation", "Time");
  }

  @Override
  public String valueRowToCSV() {
    StringBuilder sb = new StringBuilder();
    sb.append(datasetSize);
    sb.append(",");
    sb.append(nestingOperandNumberOfElements);
    sb.append(",");
    sb.append(nestingOperandSizeForEachElement);
    sb.append(",");
    sb.append(unit.getRepresentation().toString());
    sb.append(",");
    sb.append(unit.getTime());
    return sb.toString();
  }

  /**
   * Getter
   * @return dataset size
   */
  public long getDatasetSize() {
    return datasetSize;
  }

  /**
   * Setter
   * @param datasetSize new dataset size
   */
  public void setDatasetSize(long datasetSize) {
    this.datasetSize = datasetSize;
  }

  /**
   * Getter
   * @return  Number of nested graph in the operand
   */
  public long getNestingOperandNumberOfElements() {
    return nestingOperandNumberOfElements;
  }

  /**
   * Setter
   * @param nestingOperandNumberOfElements new Number of nested graph in the operand
   */
  public void setNestingOperandNumberOfElements(long nestingOperandNumberOfElements) {
    this.nestingOperandNumberOfElements = nestingOperandNumberOfElements;
  }

  /**
   * Getter
   * @return number of elements for each operands' elements
   */
  public long getNestingOperandSizeForEachElement() {
    return nestingOperandSizeForEachElement;
  }

  /**
   * Setter
   * @param nestingOperandSizeForEachElement new number of elements for each operands' elements
   */
  public void setNestingOperandSizeForEachElement(long nestingOperandSizeForEachElement) {
    this.nestingOperandSizeForEachElement = nestingOperandSizeForEachElement;
  }

  /**
   * Getter
   * @return  returns the current running time
   */
  public Time getUnit() {
    return unit;
  }

  /**
   * Setting the current running time
   * @param tu    Unit for time measurements
   * @param time  Time value
   */
  public void setUnit(TimeUnit tu, long time) {
    this.unit = new Time(tu, time);
  }

}
