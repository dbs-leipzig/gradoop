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

package org.gradoop.flink.model.impl.nested.utils;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

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

  /**
   * Gets a file from Hadoop, transforms it into a machine readable representation
   * @param ds    Hadoop data source
   * @param fif   How to serialize the given file into machine-readable elements
   * @param path  Path to the file
   * @param <T>   Class of the resulting data type
   * @return      machine readable representation
   */
  public static <T> DataSource<T> hadoopFile(HadoopDataSource ds, FileInputFormat<T> fif, String
  path) {
    return ds.getConf().getExecutionEnvironment().readFile(fif, path);
  }

  /**
   * File containing the data structure for the IdGraphDatabase, containing the vertices'
   * information
   * @param f   String from which generate the result
   * @return    the vertex file name
   */
  public static String generateVertexFile(String f) {
    return f + ".vertex";
  }

  /**
   * File containing the data structure for the IdGraphDatabase, containing the edges'
   * information
   * @param f   String from which generate the result
   * @return    the edge file name
   */
  public static String generateEdgeFile(String f) {
    return f + ".edge";
  }

}
