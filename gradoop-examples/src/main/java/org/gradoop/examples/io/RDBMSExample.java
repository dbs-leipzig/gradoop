package org.gradoop.examples.io;


import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class RDBMSExample extends AbstractRunner implements ProgramDescription {
	public static void main(String[] args) throws Exception {

//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
		// RDBMSConfig rdbmsconfig = new
		// RDBMSConfig("jdbc:mysql://localhost/employees","hr73vexy","UrlaubsReisen");
		// DataSet<Tuple4<String,String,String,String>> tables =
		// MetadataParser.parse(RDBMSMetadata.getDBMetaData(RDBMSConnect.connect(rdbmsconfig)),
		// config);
		// tables.print();
		// tables.writeAsText("/home/pc/01 Uni/8.
		// Semester/Bachelorarbeit/Test_Outputs",WriteMode.OVERWRITE);
		// env.execute();
//		DataSet<String> string = env.fromElements("hello lala");
//		string.print();
		
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> text = env.fromElements(
            "Who's there?",
            "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Integer>> wordCounts = text
            .flatMap(new LineSplitter())
            .groupBy(0)
            .sum(1);

        wordCounts.print();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
	

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return "RDBMS to Graph Convertion example";
	}

}
