/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The abstract base class for all output formats using multiple files.
 * This format will (for each record to write) determine a subdirectory, create a
 * new OutputFormat for that directory and write the record using the newly created format.
 * Those formats will be created as soon as the first record is to be written to that directory
 * and they will not be initialized automatically.
 * <br>
 * <i>Warning:</i> The {@link #configure(Configuration)}, {@link #open(int, int)},
 * {@link #close()} and {@link #initializeGlobal(int)} methods will not invoke the respective
 * methods of the new output formats, their parameters will just be stored as
 * {@link #configuration}, {@link #taskNumber}, {@link #numTasks}, {@link #parallelism}
 * respectively. Make sure to configure each output format in
 * {@link #createFormatForDirectory(Path)}.
 *
 * @param <IT> The type of the records to write.
 */
public abstract class MultipleFileOutputFormat<IT>
  implements OutputFormat<IT>, CleanupWhenUnsuccessful, InitializeOnMaster {

  /**
   * The configuration set with {@link #configure(Configuration)}.
   */
  protected Configuration configuration;

  /**
   * The write mode of this format.
   */
  protected FileSystem.WriteMode writeMode;

  /**
   * The number of this write task.
   */
  protected int taskNumber;

  /**
   * The number of tasks used by the sink.
   */
  protected int numTasks;

  /**
   * The parallelism of this format.
   */
  protected int parallelism;

  /**
   * The root output directory.
   */
  protected Path rootOutputPath;

  /**
   * Stores {@link OutputFormat}s used internally for each subdirectory determined by
   * {@link #getDirectoryForRecord(Object)}.
   */
  private Map<String, OutputFormat<IT>> formatsPerSubdirectory;

  /**
   * Creates a new output format with multiple output files.
   *
   * @param rootPath The root directory where all files will be stored.
   */
  MultipleFileOutputFormat(Path rootPath) {
    this.rootOutputPath = rootPath;
    formatsPerSubdirectory = new HashMap<>();
  }

  @Override
  public void close() throws IOException {
    for (OutputFormat<IT> outputFormat : formatsPerSubdirectory.values()) {
      outputFormat.close();
    }
    formatsPerSubdirectory.clear();
  }

  @Override
  public void configure(Configuration parameters) {
    this.configuration = parameters;
  }

  @Override
  public void initializeGlobal(int parallelism) throws IOException {
    this.parallelism = parallelism;

    // Prepare root output directory
    final FileSystem fs = rootOutputPath.getFileSystem();
    if (fs.isDistributedFS()) {
      if (!fs.initOutPathDistFS(rootOutputPath, writeMode, true)) {
        throw new IOException("Failed to initialize output root directory: " + rootOutputPath);
      }
    } else {
      if (writeMode == FileSystem.WriteMode.OVERWRITE) {
        try {
          fs.delete(rootOutputPath, true);
        } catch (IOException e) {
          throw new IOException("Could not remove existing output root directory: " +
            rootOutputPath, e);
        }
      }
      if (!fs.initOutPathLocalFS(rootOutputPath, writeMode, true)) {
        throw new IOException("Failed to initialize output root directory: " + rootOutputPath);
      }
    }
  }

  @Override
  public void open(int taskNumber, int numTasks) {
    this.taskNumber = taskNumber;
    this.numTasks = numTasks;
  }

  /**
   * Set the write mode of this format.
   *
   * @param writeMode The new write mode.
   */
  public void setWriteMode(FileSystem.WriteMode writeMode) {
    this.writeMode = writeMode;
  }

  @Override
  public void tryCleanupOnError() throws Exception {
    for (OutputFormat<IT> outputFormat : formatsPerSubdirectory.values()) {
      if (outputFormat instanceof CleanupWhenUnsuccessful) {
        ((CleanupWhenUnsuccessful) outputFormat).tryCleanupOnError();
      }
    }
    rootOutputPath.getFileSystem().delete(rootOutputPath, false);
  }


  @Override
  public void writeRecord(IT record) throws IOException {
    String subDirectory = getDirectoryForRecord(record);
    OutputFormat<IT> format;
    if (formatsPerSubdirectory.containsKey(subDirectory)) {
      format = formatsPerSubdirectory.get(subDirectory);
    } else {
      format = createFormatForDirectory(new Path(rootOutputPath, subDirectory));
      format.open(taskNumber, numTasks);
      formatsPerSubdirectory.put(subDirectory, format);
    }
    format.writeRecord(record);
  }

  /**
   * Create or load an {@link OutputFormat} to use for a specific directory.
   * The directory should be a subdirectory of the root directory of this output format.
   *
   * @param directory The (sub-)directory where the output format operates.
   * @return The output format used to write in that directory.
   * @throws IOException when the initialization of the new format fails.
   */
  protected abstract OutputFormat<IT> createFormatForDirectory(Path directory) throws IOException;

  /**
   * Get the appropriate subdirectory for a record.
   * This is expected to return a relative path from the root directory.
   *
   * @param record The record to write.
   * @return The output directory to store the record in.
   */
  protected abstract String getDirectoryForRecord(IT record);

  /**
   * Replace illegal filename characters ({@code <, >, :, ", /, \, |, ?, *}) with {@code '_'}
   * and change the string to lower case.
   *
   * @param filename filename to be cleaned
   * @return cleaned filename
   */
  public static String cleanFilename(String filename) {
    return filename.replaceAll("[<>:\"/\\\\|?*]", "_").toLowerCase();
  }
}
