package org.gradoop.flink.io.impl.rdbms.functions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;

import akka.stream.impl.Stages.SymbolicGraphStage;

public class PrintConversionPlan {
	public static void print(List<TableToNode> tablesToNodes, List<TableToEdge> tablesToEdges) {
		
		int i = 0;
		System.out.println("\nTABLES TO NODES : " + tablesToNodes.size() + "\n");
		for(TableToNode ttn : tablesToNodes) {
			System.out.println(i + " : " + ttn.getTableName() + " : " + ttn.getSqlQuery());
			for(NameTypeTuple pk : ttn.getPrimaryKeys()) {
				System.out.println(pk.f0 + " " + pk.f1.getName() + " pk");
			}
			for(FkTuple fk : ttn.getForeignKeys()) {
				System.out.println(fk.f0 + " " + fk.f1.getName() + " " + fk.f2 + " " + fk.f3 + "fk");
			}
			for(NameTypeTuple att : ttn.getFurtherAttributes()) {
				System.out.println(att.f0 + " " + att.f1.getName() + " att");
			}
			System.out.println("");
			i++;
		}
		
		System.out.println("\nTABLES TO EDGES " + tablesToEdges.size() +"\n");
		for(TableToEdge tte : tablesToEdges) {
			System.out.println(tte.getstartTableName());
			System.out.println(tte.getSqlQuery());
			System.out.println(tte.getendTableName());
			System.out.println(tte.getStartAttribute());
			System.out.println(tte.getEndAttribute() + "\n");
		}
	}
}
