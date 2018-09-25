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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * The abstract base class for all Rich output formats that are file based. Contains the logic to
 * open/close the target file streams. Feature to open several files concurrent.
 *
 * @param <IT> used output format
 *
 * references to: org.apache.flink.api.common.io.FileOutputFormat;
 */
//
// NOTE: The code in this file is based on code from the
// Apache Flink project, licensed under the Apache License v 2.0
//
// (https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api
// /common/io/FileOutputFormat.java)

@Public
public abstract class MultipleFileOutputFormat<IT>
  extends RichOutputFormat<IT> implements InitializeOnMaster, CleanupWhenUnsuccessful {

  /**
   * Behavior for creating output directories.
   */
  public static enum OutputDirectoryMode {

    /** A directory is always created, regardless of number of write tasks. */
    ALWAYS,

    /** A directory is only created for parallel output tasks,
     * i.e., number of output tasks &gt; 1.
     * If number of output tasks = 1, the output is written to a single file. */
    PARONLY
  }

  // -----------------------------------------------------------------------------

  /**
   * The key under which the name of the target path is stored in the configuration.
   */
  public static final String FILE_PARAMETER_KEY = "flink.output.file";

  /**
   * The write mode of the output.
   */
  private static WriteMode DEFAULT_WRITE_MODE;

  /**
   * The output directory mode
   */
  private static OutputDirectoryMode DEFAULT_OUTPUT_DIRECTORY_MODE;

  /**
   * Separator for directories
   */
  private static String DIRECTORY_SEPARATOR = CSVConstants.DIRECTORY_SEPARATOR;

  /**
   * The LOG for logging messages in this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(MultipleFileOutputFormat.class);

  /**
   * The path of the file to be written.
   */
  protected Path outputFilePath;

  // --------------------------------------------------------------------------------------------

  /** Map all open streams to the file name */
  protected HashMap<String, FSDataOutputStream> streams;

  /**
   * The file system for the output.
   */
  protected transient FileSystem fs;

  /** The path that is actually written to
   * (may a a file in a the directory defined by {@code outputFilePath} )
   */
  private transient Path actualFilePath;

  /** Flag indicating whether this format actually created a file,
   * which should be removed on cleanup.
   */
  private transient boolean fileCreated;

  // --------------------------------------------------------------------------------------------

  /**
   * The write mode of the output.
   */
  private WriteMode writeMode;

  /**
   * The output directory mode
   */
  private OutputDirectoryMode outputDirectoryMode;

  /**
   * Create a new instance of a MultipleFileOutputFormat.
   *
   * @param outputPath The path to the file system.
   */
  public MultipleFileOutputFormat(Path outputPath) {
    this.outputFilePath = outputPath;
    this.streams = new HashMap<>();
  }

  // --------------------------------------------------------------------------------------------

  static {
    initDefaultsFromConfiguration(GlobalConfiguration.loadConfiguration());
  }

  /**
   * Initialize defaults for output format. Needs to be a static method because it is configured
   * for local cluster execution, see LocalFlinkMiniCluster.
   *
   * @param configuration The configuration to load defaults from
   */
  private static void initDefaultsFromConfiguration(Configuration configuration) {
    final boolean overwrite = configuration.getBoolean(
        ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY,
        ConfigConstants.DEFAULT_FILESYSTEM_OVERWRITE);

    DEFAULT_WRITE_MODE = overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE;

    final boolean alwaysCreateDirectory = configuration.getBoolean(
      ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY,
      ConfigConstants.DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY);

    DEFAULT_OUTPUT_DIRECTORY_MODE = alwaysCreateDirectory ?
        OutputDirectoryMode.ALWAYS : OutputDirectoryMode.PARONLY;
  }

  //--------------------------------------------------------------------------------

  /**
   * Set the path of the output file.
   *
   * @param path The path to the output file.
   */
  public void setOutputFilePath(Path path) {
    if (path == null) {
      throw new IllegalArgumentException("Output file path may not be null.");
    }

    this.outputFilePath = path;
  }

  public Path getOutputFilePath() {
    return this.outputFilePath;
  }

  /**
   * Set the write mode of the file.
   *
   * @param mode The write mode.
   */
  public void setWriteMode(WriteMode mode) {
    if (mode == null) {
      throw new NullPointerException();
    }

    this.writeMode = mode;
  }

  public WriteMode getWriteMode() {
    return this.writeMode;
  }

  /**
   * Sets the directory mode of the output.
   *
   * @param mode The output directory mode.
   */
  public void setOutputDirectoryMode(OutputDirectoryMode mode) {
    if (mode == null) {
      throw new NullPointerException();
    }

    this.outputDirectoryMode = mode;
  }

  public OutputDirectoryMode getOutputDirectoryMode() {
    return this.outputDirectoryMode;
  }

  // ----------------------------------------------------------------

  @Override
  public void configure(Configuration parameters) {
    // get the output file path, if it was not yet set
    if (this.outputFilePath == null) {
      // get the file parameter
      String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
      if (filePath == null) {
        throw new IllegalArgumentException(
            "The output path has been specified neither via constructor/setters" +
            ", nor via the Configuration.");
      }

      try {
        this.outputFilePath = new Path(filePath);
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException(
           "Could not create a valid URI from the given file path name: " + ex.getMessage());
      }
    }

    // check if have not been set and use the defaults in that case
    if (this.writeMode == null) {
      this.writeMode = DEFAULT_WRITE_MODE;
    }

    if (this.outputDirectoryMode == null) {
      this.outputDirectoryMode = DEFAULT_OUTPUT_DIRECTORY_MODE;
    }
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    if (taskNumber < 0 || numTasks < 1) {
      throw new IllegalArgumentException("TaskNumber: " + taskNumber + ", numTasks: " + numTasks);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening stream for output (" + (taskNumber + 1) + "/" + numTasks +
          "). WriteMode=" + writeMode + ", OutputDirectoryMode=" + outputDirectoryMode);
    }

    Path p = this.outputFilePath;
    if (p == null) {
      throw new IOException("The file path is null.");
    }

    fs = p.getFileSystem();

    // if this is a local file system, we need to initialize the local output directory here
    if (!fs.isDistributedFS()) {

      if (numTasks == 1 && outputDirectoryMode == OutputDirectoryMode.PARONLY) {
        // output should go to a single file

       /* prepare local output path. checks for write mode and removes existing files
          in case of OVERWRITE mode */
        if (!fs.initOutPathLocalFS(p, writeMode, false)) {
          // output preparation failed! Cancel task.
          throw new IOException("Output path '" + p.toString() +
              "' could not be initialized. Canceling task...");
        }
      } else {
        // numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS

        if (!fs.initOutPathLocalFS(p, writeMode, true)) {
          // output preparation failed! Cancel task.
          throw new IOException("Output directory '" + p.toString() +
              "' could not be created. Canceling task...");
        }
      }
    }
  }

  /**
   * Get the stream to a specific filename. If no stream for this file already exist
   * it will create a new stream to the actual file path in the file system
   * for a file with this filename.
   *
   * Output is written to: outputPath/label/data.csv
   *
   * @param label the name of the file
   * @return the output stream for the path
   * @throws IOException - Thrown, if the output could not be opened due to an I/O problem.
   */
  public FSDataOutputStream getAndCreateFileStream(String label) throws IOException {
    if (!streams.containsKey(label)) {
      actualFilePath = new Path(
        this.outputFilePath,
        DIRECTORY_SEPARATOR + label + DIRECTORY_SEPARATOR + CSVConstants.SIMPLE_FILE);
      streams.put(label, fs.create(actualFilePath, writeMode));
    }
    return streams.get(label);
  }

  @Override
  public void close() throws IOException {
    for (Entry<String, FSDataOutputStream> e : streams.entrySet()) {
      final FSDataOutputStream s = e.getValue();
      if (s != null) {
        e.setValue(null);
        s.close();
      }
    }
  }

  /**
   * Initialization of the distributed file system if it is used.
   *
   * @param parallelism The task parallelism.
   */
  @Override
  public void initializeGlobal(int parallelism) throws IOException {
    final Path path = getOutputFilePath();
    final FileSystem fileSystem = path.getFileSystem();

    // only distributed file systems can be initialized at start-up time.
    if (fileSystem.isDistributedFS()) {

      final WriteMode wm = getWriteMode();
      final OutputDirectoryMode outDirMode = getOutputDirectoryMode();

      if (parallelism == 1 && outDirMode == OutputDirectoryMode.PARONLY) {
        // output is not written in parallel and should be written to a single file.
        // prepare distributed output path
        if (!fileSystem.initOutPathDistFS(path, wm, false)) {
          // output preparation failed! Cancel task.
          throw new IOException("Output path could not be initialized.");
        }

      } else {
        // output should be written to a directory

        // only distributed file systems can be initialized at start-up time.
        if (!fileSystem.initOutPathDistFS(path, wm, true)) {
          throw new IOException("Output directory could not be created.");
        }
      }
    }
  }

  @Override
  public void tryCleanupOnError() {
    if (this.fileCreated) {
      this.fileCreated = false;

      try {
        close();
      } catch (IOException e) {
        LOG.error("Could not properly close FileOutputFormat.", e);
      }

      try {
        FileSystem.get(this.actualFilePath.toUri()).delete(actualFilePath, false);
      } catch (FileNotFoundException e) {
        // ignore, may not be visible yet or may be already removed
      } catch (IOException ex) {
        LOG.error("Could not remove the incomplete file " + actualFilePath + '.', ex);
      }
    }
  }

  /**
   * Replace illegal filename characters (<, >, :, ", /, \, |, ?, *) with '_'
   * and change the string to lower case.
   *
   * @param filename filename to be cleaned
   * @return cleaned filename
   */
  public static String cleanFilename(String filename) {
    return filename.replaceAll("[<>:\"/\\\\|?*]", "_").toLowerCase();
  }
}
