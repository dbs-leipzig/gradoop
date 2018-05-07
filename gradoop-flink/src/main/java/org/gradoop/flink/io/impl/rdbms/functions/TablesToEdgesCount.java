package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;


public class TablesToEdgesCount implements FlatMapFunction<Tuple4<String,String,String,String>, Tuple2<String,Integer>>{

	@Override
	public void flatMap(Tuple4<String, String, String, String> in, Collector<Tuple2<String,Integer>> out)
			throws Exception {
		// TODO Auto-generated method stub
		out.collect(new Tuple2<>(in.f0,1));
	}
}
