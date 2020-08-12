package org.gradoop.examples.matching.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.gradoop.examples.matching.TemporalQueries;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;

import java.io.FileOutputStream;

public class QueryTranslation {
  /**
   * Command line options for the runner.
   */
  protected static final Options OPTIONS = new Options();
  /**
   * Option to declare the query
   */
  private static final String OPTION_QUERY = "q";
  /**
   * Option for output file (result is appended)
   */
  private static final String OPTION_OUTPUT = "o";
  /**
   * Option for upper bound for query
   */
  private static final String OPTION_UPPER = "u";
  /**
   * Option for lower bound for query
   */
  private static final String OPTION_LOWER = "l";
  /**
   * The query
   */
  private static String QUERY;
  /**
   * Output file
   */
  private static String OUTPUT;
  /**
   * Upper bound
   */
  private static String UPPER;
  /**
   * Lower bound
   */
  private static String LOWER;

  static {
    OPTIONS.addOption(OPTION_QUERY, "query", true,
      "query to transform");
    OPTIONS.addOption(OPTION_OUTPUT, "out", true,
      "output file");
    OPTIONS.addOption(OPTION_UPPER, "upper", true,
      "upper bound for query");
    OPTIONS.addOption(OPTION_LOWER, "lower", true,
      "lower bound for query");
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args);

    if (cmd == null) {
      return;
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    readCMDArguments(cmd);

    TemporalQueryHandler handler = new TemporalQueryHandler(QUERY);
    String processed = handler.getCNF().toString();

    writeToFile(processed);
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    if (!cmd.hasOption(OPTION_QUERY)) {
      throw new IllegalArgumentException("Query missing");
    }
    if (!cmd.hasOption(OPTION_OUTPUT)) {
      throw new IllegalArgumentException("Output file missing");
    }
    if(cmd.hasOption(OPTION_LOWER) ^ cmd.hasOption(OPTION_UPPER)){
      throw new IllegalArgumentException("either lower and upper bound or no " +
        "bounds must be given");
    }
  }

  protected static CommandLine parseArguments(String[] args)
    throws ParseException {
    return new DefaultParser().parse(OPTIONS, args);
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    OUTPUT = cmd.getOptionValue(OPTION_OUTPUT);
    QUERY = cmd.getOptionValue(OPTION_QUERY);
    LOWER = cmd.hasOption(OPTION_LOWER) ? cmd.getOptionValue(OPTION_LOWER) : null;
    UPPER = cmd.hasOption(OPTION_UPPER) ? cmd.getOptionValue(OPTION_UPPER) : null;
    if(QUERY.equals("q1")){
      QUERY = LOWER==null ? TemporalQueries.q1(LOWER, UPPER, true) :
        TemporalQueries.q1("1970-01-01", "2100-01-01", true);
    }
    System.out.println(QUERY);
  }

  private static void writeToFile(String cnf) throws Exception{
    FileOutputStream fos = new FileOutputStream(OUTPUT, true);
    fos.write((cnf+"\n").getBytes());
    fos.close();
  }

}
