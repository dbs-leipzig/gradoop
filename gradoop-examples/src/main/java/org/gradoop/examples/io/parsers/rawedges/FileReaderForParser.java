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


package org.gradoop.examples.io.parsers.rawedges;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.io.parsers.ParametricInputFormat;

/**
 * Defining a generic reader.
 * TODO: there are some better ways to read from a file
 */
public class FileReaderForParser {

  /**
   * Environment where the file has to be read
   */
  private ExecutionEnvironment env;

  /**
   * Path to the file
   */
  private String file;

  /**
   * Standard string reader
   */
  private ParametricInputFormat pif;

  /**
   * Default constructor
   */
  public FileReaderForParser() {
    this.env = ExecutionEnvironment.getExecutionEnvironment();
    this.pif = new ParametricInputFormat();
  }

  /**
   * Default constructor, wher ethe ParametricInputFormat has a specified delimiter
   * @param delimiter   Delimiter String
   */
  public FileReaderForParser(String delimiter) {
    this();
    this.pif.setDelimiter(delimiter);
  }

  /**
   * Source file where to read the data
   * @param file  overmentioned file
   * @return      Updated instance of this
   * @param <X>   Element extending the FileReaderForParser
   */
  public <X extends FileReaderForParser> X fromFile(String file) {
    this.file = file;
    return (X) this;
  }

  /**
   * Reads the source line by line
   * @return the aforementioned splitted datasource
   */
  public DataSet<String> readAsStringDataSource() {
    return env.readFile(pif, file);
  }

  /**
   * Sets the line/block delimiter for reading the file
   * @param delimiter such delimiter
   */
  public void setDelimiter(String delimiter) {
    this.pif.setDelimiter(delimiter);
  }


  /**
   * Setting the running environment
   * @param env The environment
   */
  public void setEnvironment(ExecutionEnvironment env) {
    this.env = env;
  }
}
