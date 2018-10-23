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
package org.gradoop.flink.io.impl.csv.indexed.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.tuples.CSVElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * This is an OutputFormat to serialize {@link Tuple}s to text by there labels.
 * The output is structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 *
 * @param <T> Tuple that will be written to csv
 *
 * references to: org.apache.flink.api.java.io.CSVOutputFormat
 */
public class IndexedCSVFileFormat<T extends Tuple> extends MultipleFileOutputFormat<T> {

  /**
   * The default line delimiter if no one is set.
   */
  public static final String DEFAULT_LINE_DELIMITER = CSVConstants.ROW_DELIMITER;

  /**
   * The default field delimiter if no is set.
   */
  public static final String DEFAULT_FIELD_DELIMITER = CSVConstants.TOKEN_DELIMITER;

  /**
   * The key under which the name of the target path is stored in the configuration.
   */
  public static final String FILE_PARAMETER_KEY = "flink.output.file";

  // --------------------------------------------------------------------------------

  /**
   * The logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(IndexedCSVFileFormat.class);

  // --------------------------------------------------------------------------------

  /**
   * The character the fields should be separated.
   */
  private String fieldDelimiter;

  /**
   * The character the lines should be separated.
   */
  private String recordDelimiter;

  /**
   * If null values should be written.
   */
  private boolean allowNullValues = true;

  /**
   * If the entries should be written in quotes.
   */
  private boolean quoteStrings = false;

  /**
   * Map the lable of the tuple to a writer.
   */
  private HashMap<String, Writer> labelsToWriter;

  /**
   * The charset that is uses for the output encoding.
   */
  private String charsetName;

  /**
   * Creates a new instance of an IndexedCSVFileFormat. Use the default record delimiter '\n'
   * and the default field delimiter ','.
   *
   * @param outputPath The path where the CSV file will be written.
   */
  public IndexedCSVFileFormat(Path outputPath) {
    this(outputPath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER);
  }

  /**
   * Creates a new instance of an IndexedCSVFileFormat. Use the default record delimiter '\n'.
   *
   * @param outputPath The path where the CSV file will be written.
   * @param fieldDelimiter The field delimiter for the CSV file.
   */
  public IndexedCSVFileFormat(Path outputPath, String fieldDelimiter) {
    this(outputPath, DEFAULT_LINE_DELIMITER, fieldDelimiter);
  }

  /**
   * Creates a new instance of an IndexedCSVFileFormat.
   *
   * @param outputPath The path where the CSV file will be written.
   * @param recordDelimiter The record delimiter for the CSV file.
   * @param fieldDelimiter The field delimiter for the CSV file.
   */
  public IndexedCSVFileFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
    super(outputPath);
    if (recordDelimiter == null) {
      throw new IllegalArgumentException("RecordDelmiter shall not be null.");
    }

    if (fieldDelimiter == null) {
      throw new IllegalArgumentException("FieldDelimiter shall not be null.");
    }

    this.fieldDelimiter = fieldDelimiter;
    this.recordDelimiter = recordDelimiter;
    this.labelsToWriter = new HashMap<>();
  }

  /**
   * Check the label of a tuple to identify the name of the according output file.
   * If there is not already a writer for the label, it will map it to the tuple.
   *
   * @param fileName Name of the file for the tuple.
   * @param tuple Tuple that will be mapped to writer.
   * @throws IOException - Throne if creating writer or output stream fails.
   */
  public void mapWriter(Tuple tuple, String fileName) throws IOException {
    if (labelsToWriter.containsKey(fileName)) {
      writeToCSV(tuple, labelsToWriter.get(fileName));
    } else {
      FSDataOutputStream stream = super.getAndCreateFileStream(fileName);
      Writer wrt = this.charsetName == null ? new OutputStreamWriter(
          new BufferedOutputStream(stream, 4096), "UTF8") :
            new OutputStreamWriter(new BufferedOutputStream(stream, 4096), this.charsetName);
      labelsToWriter.put(fileName, wrt);
      writeToCSV(tuple, wrt);
    }
  }

  /**
   * Write every field of the tuple in a csv file.
   * Separates record via record delimiter and every field in a record
   * with field delimiters.
   *
   * @param tuple the tuple to write in the output file
   * @param writer the writer for the tuple
   * @throws IOException - Thrown, if the records could not be added to to an I/O problem.
   */
  public void writeToCSV(Tuple tuple, Writer writer) throws IOException {

    int numFields = tuple.getArity();
    for (int i = 0; i < numFields; i++) {
      Object v = tuple.getField(i);
      if (v != null) {
        if (i != 0) {
          writer.write(this.fieldDelimiter);
        }

        if (quoteStrings) {
          if (v instanceof String || v instanceof StringValue) {
            writer.write('"');
            writer.write(v.toString());
            writer.write('"');
          } else {
            writer.write(v.toString());
          }
        } else {
          writer.write(v.toString());
        }
      } else {
        if (this.allowNullValues) {
          if (i != 0) {
            writer.write(this.fieldDelimiter);
          }
        } else {
          throw new RuntimeException("Cannot write tuple with <null> value at position: " + i);
        }
      }
    }
    writer.write(this.recordDelimiter);
  }

  @Override
  public void writeRecord(T record) throws IOException {
    String label = ((CSVElement) record).getLabel();
    if (label.isEmpty()) {
      label = CSVConstants.DEFAULT_DIRECTORY;
    } else {
      label = cleanFilename(label);
    }
    mapWriter(record, label);
  }

  @Override
  public void close() throws IOException {
    if (labelsToWriter != null) {
      for (Entry<String, Writer> entry : labelsToWriter.entrySet()) {
        entry.getValue().flush();
        entry.getValue().close();
      }
    }
    super.close();
  }

  /**
   * Sets the charset with which the CSV strings are written to the file.
   * If not specified, the output format uses the systems default character encoding.
   *
   * @param charsetName The name of charset to use for encoding the output.
   */
  public void setCharsetName(String charsetName) {
    this.charsetName = charsetName;
  }
}

