/**
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.gradoop.flink.io.impl.csv.tuples.CSVEdge;
import org.gradoop.flink.io.impl.csv.tuples.CSVVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an OutputFormat to serialize {@link org.apache.flink.api.java.tuple.Tuple}s to text by there labels.
 * The output is structured by record delimiters and field delimiters as common in CSV files.
 *Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 * @param <T>
 *
 * references to: org.apache.flink.api.java.io.CSVOutputFormat
 */
public class IndexedCSVFileFormat<T extends Tuple> extends MultipleFileOutputFormat<T> {

  /**
   * The logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(IndexedCSVFileFormat.class);

  // --------------------------------------------------------------------------------

  /**
   * The default line delimiter if no one is set.
   */
  public static final String DEFAULT_LINE_DELIMITER = IndexedCSVFileFormat.DEFAULT_LINE_DELIMITER;

  /**
   * The default field delimiter if no is set.
   */
  public static final String DEFAULT_FIELD_DELIMITER = String.valueOf(
      IndexedCSVFileFormat.DEFAULT_FIELD_DELIMITER);

  /**
   * The key under which the name of the target path is stored in the configuration.
   */
  public static final String FILE_PARAMETER_KEY = "flink.output.file";

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
  private Map<String, Writer> labelsToWriter;

  /**
   * List with all ready existing labels
   */
  private ArrayList<String> objectLabels;

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
    this.objectLabels = new ArrayList<>();
    this.labelsToWriter = new HashMap<>();
  }

  /**
   * Get the label of a vertex to identify the name of the according output file.
   * If there is not all ready a writer for the label, it will be created an
   * map the vertex data to it.
   *
   * @param vertex that will be mapped to writer.
   * @throws IOException - Throne if creating writer or output stream fails.
   */
  public void mapWriterToCSVVertex(CSVVertex vertex) throws IOException {
    String label = vertex.getLabel();
    if (objectLabels.contains(label)) {
      writeToCSV(vertex, labelsToWriter.get(label));
    } else {
      FSDataOutputStream stream = super.getFileStream(label);
      Writer wrt = new OutputStreamWriter(new BufferedOutputStream(stream, 4096));
      labelsToWriter.put(label, wrt);
      objectLabels.add(label);
      writeToCSV(vertex, wrt);
    }
  }

  /**
   * Get the label of an edge to identify the name of the according output file.
   * If there is not all ready a writer for the label, it will be created an
   * map the edge data to it.
   *
   * @param edge that will be mapped to writer.
   * @throws IOException - Throne if creating writer or output stream fails.
   */
  public void mapWriterToCSVEdge(CSVEdge edge) throws IOException {
    String label = edge.getLabel();
    if (objectLabels.contains(label)) {
      writeToCSV(edge, labelsToWriter.get(label));
    } else {
      FSDataOutputStream stream = super.getFileStream(label);
      Writer wrt = new OutputStreamWriter(new BufferedOutputStream(stream, 4096));
      labelsToWriter.put(label, wrt);
      objectLabels.add(label);
      writeToCSV(edge, wrt);
    }
  }

  /**
   * Write every field of the tuple in a csv file.
   * Separates record via record delimiter and every field in a record
   * with field delimiters.
   * @param t the tuple to write in the output file
   * @param wrt the writer for the tuple
   * @throws IOException - Thrown, if the records could not be added to to an I/O problem.
   */
  public void writeToCSV(Tuple t, Writer wrt) throws IOException { //, boolean firstObject

    int numFields = t.getArity();
    for (int i = 0; i < numFields; i++) {
      Object v = t.getField(i);
      if (v != null) {
        if (i != 0) {
          wrt.write(this.fieldDelimiter);
        }

        if (quoteStrings) {
          if (v instanceof String || v instanceof StringValue) {
            wrt.write('"');
            wrt.write(v.toString());
            wrt.write('"');
          } else {
            wrt.write(v.toString());
          }
        } else {
          wrt.write(v.toString());
        }
      } else {
        if (this.allowNullValues) {
          if (i != 0) {
            wrt.write(this.fieldDelimiter);
          }
        } else {
          throw new RuntimeException("Cannot write tuple with <null> value at position: " + i);
        }
      }
    }
    wrt.write(this.recordDelimiter);
  }

  @Override
  public void writeRecord(T record) throws IOException {
    if (record.getClass().equals(CSVVertex.class)) {
      mapWriterToCSVVertex((CSVVertex) record);
    } else if (record.getClass().equals(CSVEdge.class)) {
      mapWriterToCSVEdge((CSVEdge) record);
    }
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
}

