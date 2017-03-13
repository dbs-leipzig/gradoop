package org.gradoop.flink.io.reader.parsers.utilities;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.reader.parsers.ParametricInputFormat;

/**
 * Created by vasistas on 12/03/17.
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
    return (X)this;
  }

  public DataSet<String> readAsStringDataSource() {
    return env.readFile(pif, file);
  }

  public void setDelimiter(String delimiter) {
    this.pif.setDelimiter(delimiter);
  }
}
