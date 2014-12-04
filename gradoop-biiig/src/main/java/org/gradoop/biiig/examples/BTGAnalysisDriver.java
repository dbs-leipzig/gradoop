package org.gradoop.biiig.examples;

import org.apache.commons.cli.ParseException;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.algorithms.PairAggregator;
import org.gradoop.algorithms.SelectAndAggregate;
import org.gradoop.biiig.BIIIGConstants;
import org.gradoop.biiig.algorithms.BTGComputation;
import org.gradoop.biiig.io.formats.BTGHBaseVertexInputFormat;
import org.gradoop.biiig.io.formats.BTGHBaseVertexOutputFormat;
import org.gradoop.biiig.io.reader.FoodBrokerReader;
import org.gradoop.io.formats.PairWritable;
import org.gradoop.io.reader.BulkLoadEPG;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Vertex;
import org.gradoop.model.operators.VertexDoubleAggregate;
import org.gradoop.model.operators.VertexPredicate;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;

import java.io.IOException;

/**
 * Runs the BIIIG Foodbroker/BTG Example
 */
public class BTGAnalysisDriver extends Configured implements Tool {
  private static Logger LOG = Logger.getLogger(BTGAnalysisDriver.class);

  /**
   * args[0] - bulk load input graph file
   * args[1] - bulk load output folder
   * args[2] - worker count
   * args[3] - scan cache for HBase
   *
   * @param args driver arguments
   * @return Exit code (0 - ok)
   * @throws Exception
   */
  @Override
  public int run(String[] args)
    throws Exception {
    if (args.length != 4) {
      System.err.printf("Usage: %s [generic options] <graph-input-file> " +
          "<bulk-load-output-dir> <giraph-worker-count> <scan-cache-size>%n",
        getClass()
          .getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
    }

    Configuration conf = getConf();

    /*
    Step 0: Create HBase tables
     */
    HBaseGraphStoreFactory.createGraphStore(conf,
      new EPGVertexHandler(),
      new EPGGraphHandler()
    );

    /*
    Step 1: Bulk Load of the graph into HBase using MapReduce
     */
    if (!runBulkLoad(conf, args[0], args[1])) {
      return -1;
    }

    /*
    Step 2: BTG Computation using Giraph
     */
    if (!runBTGComputation(conf, Integer.parseInt(args[2]))) {
      return -1;
    }

    /*
    Step 3: Select And Aggregate using MapReduce
     */
    if (!runSelectAndAggregate(conf, Integer.parseInt(args[3]))) {
      return -1;
    }

    return 0;
  }

  /**
   * Runs the HFile conversion from the given file to the output dir. Also
   * loads the HFiles to region servers.
   *
   * @param conf      Cluster config
   * @param graphFile input file in HDFS
   * @param outDir    HFile output dir in HDFS
   * @throws IOException
   */
  private boolean runBulkLoad(Configuration conf, String graphFile, String
    outDir)
    throws Exception {
    Path inputFile = new Path(graphFile);
    Path outputDir = new Path(outDir);

    // set line reader to read lines in input splits
    conf.setClass(BulkLoadEPG.VERTEX_LINE_READER,
      FoodBrokerReader.class,
      VertexLineReader.class);
    // set vertex handler that creates the Puts
    conf.setClass(BulkLoadEPG.VERTEX_HANDLER,
      EPGVertexHandler.class,
      VertexHandler.class);

    Job job = new Job(conf, BulkLoadEPG.class.getName());
    // mapper that runs the HFile conversion
    job.setMapperClass(BulkLoadEPG.class);
    // input format for Mapper (File)
    job.setInputFormatClass(TextInputFormat.class);
    // output Key class of Mapper
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    // output Value class of Mapper
    job.setMapOutputValueClass(Put.class);

    // set input file
    FileInputFormat.addInputPath(job, inputFile);
    // set output directory
    FileOutputFormat.setOutputPath(job, outputDir);

    HTable hTable = new HTable(conf, GConstants.DEFAULT_TABLE_VERTICES);

    // auto configure partitioner and reducer corresponding to the number of
    // regions
    HFileOutputFormat2.configureIncrementalLoad(job, hTable);
    boolean successful;

    // run job
    successful = job.waitForCompletion(true);
    if (!successful) {
      LOG.error("Error during bulk import ... stopping pipeline");
      return false;
    }

    // load created HFiles to the region servers
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(outputDir, hTable);

    return true;
  }

  /**
   * Runs the BTG computation on the input graph using Giraph.
   *
   * @param conf        Cluster configuration
   * @param workerCount Number of workers Giraph shall use.
   * @return true, if the job completed successfully, false otherwise
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws ParseException
   */
  private boolean runBTGComputation(Configuration conf, int workerCount)
    throws IOException, ClassNotFoundException, InterruptedException,
    ParseException {
    // set HBase table to read graph from
    conf.set(TableInputFormat.INPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);
    // set HBase table to write computation results to
    conf.set(TableOutputFormat.OUTPUT_TABLE, GConstants.DEFAULT_TABLE_VERTICES);

    // setup Giraph job
    GiraphJob job = new GiraphJob(conf, BTGComputation.class.getName());
    GiraphConfiguration giraphConf = job.getConfiguration();


    giraphConf.setComputationClass(BTGComputation.class);
    giraphConf.setVertexInputFormatClass(BTGHBaseVertexInputFormat.class);
    giraphConf.setVertexOutputFormatClass(BTGHBaseVertexOutputFormat.class);
    if (isRunningDistributed()) {
      giraphConf.setWorkerConfiguration(workerCount, workerCount, 100f);
    } else {
      giraphConf.setWorkerConfiguration(1, 1, 100.0F);
      GiraphConstants.SPLIT_MASTER_WORKER.set(giraphConf, false);
      GiraphConstants.LOCAL_TEST_MODE.set(giraphConf, true);
    }

    return job.run(true);
  }

  private boolean runSelectAndAggregate(Configuration conf, int scanCache)
    throws IOException, ClassNotFoundException, InterruptedException {
    /*
     mapper settings
      */
    // vertex handler
    conf.setClass(GConstants.VERTEX_HANDLER_CLASS,
      EPGVertexHandler.class,
      VertexHandler.class
    );

    // vertex predicate
    conf.setClass(SelectAndAggregate.VERTEX_PREDICATE_CLASS,
      SalesOrderPredicate.class,
      VertexPredicate.class
    );

    // vertex aggregate
    conf.setClass(SelectAndAggregate.VERTEX_AGGREGATE_CLASS,
      ProfitVertexAggregate.class,
      VertexDoubleAggregate.class);

    /*
    reducer settings
     */
    // graph handler
    conf.setClass(GConstants.GRAPH_HANDLER_CLASS,
      EPGGraphHandler.class,
      GraphHandler.class
    );
    // pair aggregate class
    conf.setClass(SelectAndAggregate.PAIR_AGGREGATE_CLASS,
      SumAgregator.class,
      PairAggregator.class);

    Job job = new Job(conf, SelectAndAggregate.class.getName());
    Scan scan = new Scan();
    scan.setCaching(scanCache);
    scan.setCacheBlocks(false);

    // map
    TableMapReduceUtil.initTableMapperJob(
      GConstants.DEFAULT_TABLE_VERTICES,
      scan,
      SelectAndAggregate.SelectMapper.class,
      LongWritable.class,
      PairWritable.class,
      job
    );

    // reduce
    TableMapReduceUtil.initTableReducerJob(
      GConstants.DEFAULT_TABLE_GRAPHS,
      SelectAndAggregate.AggregateReducer.class,
      job
    );

    // run
    return job.waitForCompletion(true);
  }

  public static class SalesOrderPredicate implements VertexPredicate {

    private static final String KEY = BIIIGConstants.META_PREFIX + "class";
    private static final String VALUE = "SalesOrder";

    @Override
    public boolean evaluate(Vertex vertex) {
      boolean result = false;
      if (vertex.getPropertyCount() > 0) {
        Object o = vertex.getProperty(KEY);
        result = (o != null && o.equals(VALUE));
      }
      return result;
    }
  }

  public static class ProfitVertexAggregate implements VertexDoubleAggregate {
    private static final String EXPENSE_KEY = "expense";
    private static final String REVENUE_KEY = "revenue";


    @Override
    public Double aggregate(Vertex vertex) {
      double calcValue = 0f;
      if (vertex.getPropertyCount() > 0) {
        Object o = vertex.getProperty(REVENUE_KEY);
        if (o != null) {
          calcValue += (double) o;
        }
        o = vertex.getProperty(EXPENSE_KEY);
        if (o != null) {
          calcValue -= (double) o;
        }
      }
      return calcValue;
    }
  }

  public static class SumAgregator implements PairAggregator {

    @Override
    public Pair<Boolean, Double> aggregate(Iterable<PairWritable> values) {
      double sum = 0;
      boolean predicate = false;
      for (PairWritable value : values) {
        sum = sum + value.getValue().get();
        if (value.getPredicateResult().get()) {
          predicate = true;
        }
      }
      return new Pair<>(predicate, sum);
    }
  }

  private boolean isRunningDistributed() {
    return System.getProperty("prop.mapred.job.tracker") != null;
  }
}
