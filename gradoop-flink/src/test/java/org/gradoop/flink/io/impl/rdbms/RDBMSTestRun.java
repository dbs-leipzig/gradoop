package org.gradoop.flink.io.impl.rdbms;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class RDBMSTestRun {
	public static void main(String[] args){
		Result result = JUnitCore.runClasses(RDBMSDataSourceTest.class);
		
		for(Failure failure : result.getFailures()){
			System.out.println(failure.toString());
		}
		
		System.out.println(result.wasSuccessful());
	}
}
