
package org.gradoop.flink.io.impl.csv;

/**
 * Constants needed for CSV parsing.
 */
public class CSVConstants {
  /**
   * Used to separate the tokens (id, label, values) in the CSV file.
   */
  public static final String TOKEN_DELIMITER = ";";
  /**
   * Used to separate the property values in the CSV file.
   */
  public static final String VALUE_DELIMITER = "|";
  /**
   * Used to separate lines in the output CSV files.
   */
  public static final String ROW_DELIMITER = System.getProperty("line.separator");
}
