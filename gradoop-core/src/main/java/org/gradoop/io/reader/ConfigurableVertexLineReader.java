package org.gradoop.io.reader;

import org.apache.hadoop.conf.Configuration;

/**
 * Used to read a vertex from an input string and other Configuration Params.
 */
public interface ConfigurableVertexLineReader extends VertexLineReader {
  /**
   * Used to set a Configuration Object
   *
   * @param conf the conf object
   */
  void setConf(final Configuration conf);
}
