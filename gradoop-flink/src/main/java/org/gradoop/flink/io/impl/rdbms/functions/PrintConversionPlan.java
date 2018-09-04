package org.gradoop.flink.io.impl.rdbms.functions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;

import akka.stream.impl.Stages.SymbolicGraphStage;

/**
 * Prints a conversion plan 
 *
 */
public class PrintConversionPlan {
	
	/**
	 * Prints tables to nodes and tables to edges to file
	 * @param tablesToNodes Representation of tables going to convert to vertices
	 * @param tablesToEdges Representation of foreign key and n:m relations going to convert to edges
	 * @param outputPath File path where plan will be stored
	 */
	public static void print(ArrayList<TableToNode> tablesToNodes, ArrayList<TableToEdge> tablesToEdges,
			String outputPath) {
		
		try {
			PrintWriter pw = new PrintWriter(new File(outputPath));

			pw.println("TABLES TO NODES");
			pw.println("---------------\n");
			for (TableToNode ttn : tablesToNodes) {
				pw.println("Table: " + ttn.getTableName() + "\n");
				for (NameTypeTuple pk : ttn.getPrimaryKeys()) {
					pw.println(pk.f0 + " " + pk.f1.getName() + " : pk");
				}
				for (FkTuple fk : ttn.getForeignKeys()) {
					pw.println(fk.f0 + " " + fk.f1.getName() + " " + fk.f2 + " " + fk.f3 + " : fk");
				}
				for (NameTypeTuple att : ttn.getFurtherAttributes()) {
					pw.println(att.f0 + " " + att.f1.getName() + " : att");
				}
				pw.println("\n");
			}
			pw.println("\nTABLES TO EDGES");
			pw.println("-----------------\n");
			for (TableToEdge tte : tablesToEdges) {
				pw.println("Starttable: " + tte.getstartTableName());
				pw.println("Startattribute: " + tte.getStartAttribute());
				pw.println("Endtable : " + tte.getendTableName());
				pw.println("Endattribute : " + tte.getEndAttribute());
				pw.println("Directed Edge: " + tte.isDirectionIndicator() + "\n");
			}
			pw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
