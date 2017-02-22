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

package org.gradoop.examples.dimspan;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;

/**
 * A program to run DIMSpan standalone.
 */
public class DIMSpanRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Option to set path to input file
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to set path to output file
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to set minimum support threshold
   */
  public static final String OPTION_MIN_SUPPORT = "ms";

  /**
   * Option to enable undirected mining mode
   */
  public static final String OPTION_UNDIRECTED_MODE = "u";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true, "Path to input file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true, "Path to output file");
    OPTIONS.addOption(OPTION_MIN_SUPPORT, "min-support", true, "Minimum support threshold");
    OPTIONS.addOption(OPTION_UNDIRECTED_MODE, "undirected", false, "Enable undirected mode");
  }

  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, DIMSpanRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments from command line
    final String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);

    float minSupport = Float.valueOf(cmd.getOptionValue(OPTION_MIN_SUPPORT));

    boolean directed = !cmd.hasOption(OPTION_UNDIRECTED_MODE);

    DIMSpanConfig fsmConfig = new DIMSpanConfig(minSupport, directed);

    System.out.println(inputPath + outputPath + fsmConfig.toString());
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("No input file specified.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("No output file specified.");
    }
    if (!cmd.hasOption(OPTION_MIN_SUPPORT)) {
      throw new IllegalArgumentException("No min support specified.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return DIMSpanRunner.class.getName();
  }
}
