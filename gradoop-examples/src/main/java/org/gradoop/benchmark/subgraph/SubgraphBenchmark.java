package org.gradoop.benchmark.subgraph;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.functions.EdgeLabelFilter;
import org.gradoop.common.functions.VertexLabelFilter;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized subgraph benchmark.
 */
public class SubgraphBenchmark extends AbstractRunner implements ProgramDescription {
    /**
     * Option to declare path to input graph
     */
    private static final String OPTION_INPUT_PATH = "i";
    /**
     * Option to declare path to output graph
     */
    private static final String OPTION_OUTPUT_PATH = "o";
    /**
     * Option to declare path to statistics csv file
     */
    private static final String OPTION_CSV_PATH = "c";
    /**
     * Option to declare verification
     */
    private static final String OPTION_VERIFICATION = "v";
    /**
     * Used input path
     */
    private static String INPUT_PATH;
    /**
     * Used output path
     */
    private static String OUTPUT_PATH;
    /**
     * Used csv path
     */
    private static String CSV_PATH;
    /**
     * Used verification flag
     */
    private static boolean VERIFICATION;

    static {
        OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
          "Path to source files.");
        OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
          "Path to output file");
        OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
          "Path to csv statistics");
        OPTIONS.addOption(OPTION_VERIFICATION, "verification", false,
          "Verify Subgraph with join.");
    }


    /**
     * Main program to run the benchmark. Arguments are the available options.
     *
     * @param args program arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArguments(args, SubgraphBenchmark.class.getName());

        if (cmd == null) {
            System.exit(1);
        }

        // test if minimum arguments are set
        performSanityCheck(cmd);

        // read cmd arguments
        readCMDArguments(cmd);

        // create gradoop config
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

        // read graph
        DataSource source = new IndexedCSVDataSource(INPUT_PATH, conf);

        LogicalGraph graph = source.getLogicalGraph();

        // compute subgraph -> verify results (join) vs no verify (filter)
        LogicalGraph subGraph;

        if (VERIFICATION) {
            subGraph = graph.subgraph(
              new VertexLabelFilter("person"),
              new EdgeLabelFilter("knows"),
              Subgraph.Strategy.BOTH_VERIFIED);
        } else {
            subGraph = graph.subgraph(
              new VertexLabelFilter("person"),
              new EdgeLabelFilter("knows"),
              Subgraph.Strategy.BOTH);
        }

        // write graph
        DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
        sink.write(subGraph);

        env.execute();
        writeCSV(env);
    }

    /**
     * Reads the given arguments from command line
     *
     * @param cmd command line
     */
    private static void readCMDArguments(CommandLine cmd) {
        // read input output paths
        INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
        OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH);
        CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);
        // set subgraph verification flag
        VERIFICATION = cmd.hasOption(OPTION_VERIFICATION);
    }

    /**
     * Checks if the minimum of arguments is provided
     *
     * @param cmd command line
     */
    private static void performSanityCheck(CommandLine cmd) {
        if (!cmd.hasOption(OPTION_INPUT_PATH)) {
            throw new IllegalArgumentException("Define a graph input directory.");
        }
        if (!cmd.hasOption(OPTION_CSV_PATH)) {
            throw new IllegalArgumentException("Path to CSV-File need to be set.");
        }
        if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
            throw new IllegalArgumentException("Define a graph output directory.");
        }
    }

    /**
     * Method to create and add lines to a csv-file
     * @throws IOException
     */
    private static void writeCSV(ExecutionEnvironment env) throws IOException {

        String head = String.format("%s|%s|%s|%s%n",
                "Parallelism",
                "dataset",
                "verification",
                "Runtime(s)");

        String tail = String.format("%s|%s|%s|%s%n",
                env.getParallelism(),
                INPUT_PATH,
                VERIFICATION,
                env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

        File f = new File(CSV_PATH);
        if (f.exists() && !f.isDirectory()) {
            FileUtils.writeStringToFile(f, tail, true);
        } else {
            PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
            writer.print(head);
            writer.print(tail);
            writer.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return SubgraphBenchmark.class.getName();
    }
}
