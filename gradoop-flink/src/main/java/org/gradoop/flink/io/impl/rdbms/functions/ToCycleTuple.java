package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ToCycleTuple implements FlatMapFunction<Tuple2<String, String>, Tuple3<String,Integer,Integer>> {
	
	@Override
	public void flatMap(Tuple2<String, String> in, Collector<Tuple3<String,Integer,Integer>> out) throws Exception {
		// TODO Auto-generated method stub
		out.collect(new Tuple3<>(in.f0,0,0));
	}
}
