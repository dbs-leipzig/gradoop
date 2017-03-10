package org.gradoop.flink.model.impl.nested.utils;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.api.common.io.*;
import org.gradoop.flink.model.impl.nested.HadoopDataSource;

/**
 * Utility functions for handling FileSystems in ApacheFlink
 */
public class FileSystemUtils {

  /**
   * Converts the boolean constant into the actual overwrite mode
   * @param doOverwrite If the file has to be overwritten or not
   * @return  The actual constant used by Apache Flink
   */
  public static FileSystem.WriteMode overwrite(boolean doOverwrite) {
    return doOverwrite ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;
  }

  public static <T> DataSource<T> hadoopFile(HadoopDataSource ds, FileInputFormat<T> fif, String
  path) {
    return ds.getConf().getExecutionEnvironment().readFile(fif,path);
  }

  public static String generateVertexFile(String f) {
    return f+".vertex";
  }

  public static String generateEdgeFile(String f) {
    return f+".edge";
  }

}
