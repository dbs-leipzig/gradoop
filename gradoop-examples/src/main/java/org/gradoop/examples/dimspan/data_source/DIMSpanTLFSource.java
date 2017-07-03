
package org.gradoop.examples.dimspan.data_source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.io.impl.tlf.inputformats.TLFInputFormat;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Lightweight data source for TLF formatted string-labeled graphs.
 * NOTE, no consistency check, inconsistent data will cause errors!
 */
public class DIMSpanTLFSource {
  /**
   * Gradoop configuration
   */
  private final GradoopFlinkConfig config;
  /**
   * input file path
   */
  private final String filePath;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS(hdfs://).
   *
   * @param filePath input file path
   * @param config Gradoop configuration
   */
  public DIMSpanTLFSource(String filePath, GradoopFlinkConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (filePath == null) {
      throw new IllegalArgumentException("vertex file must not be null");
    }

    this.filePath = filePath;
    this.config = config;
  }

  /**
   * Reads the input as dataset of TLFGraphs.
   *
   * @return io graphs
   */
  public DataSet<LabeledGraphStringString> getGraphs() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();
    return env
      .readHadoopFile(new TLFInputFormat(), LongWritable.class, Text.class, getFilePath())
      .map(new DIMSpanGraphFromText());
  }

  // GETTERS AND SETTERS

  private GradoopFlinkConfig getConfig() {
    return config;
  }

  private String getFilePath() {
    return filePath;
  }
}
