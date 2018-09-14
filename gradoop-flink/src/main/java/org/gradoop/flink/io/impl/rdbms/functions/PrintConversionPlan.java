/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.io.impl.rdbms.functions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.io.impl.rdbms.tuples.FkTuple;
import org.gradoop.flink.io.impl.rdbms.tuples.NameTypeTuple;

/**
 * Prints a conversion plan 
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

			pw.println("TABLES TO NODES :" + tablesToNodes.size() );
			pw.println("---------------\n");
			for (TableToNode ttn : tablesToNodes) {
				pw.println("Table: " + ttn.getTableName() + " : " + ttn.getSqlQuery() + "\n");
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
			pw.println("\nTABLES TO EDGES :" + tablesToEdges.size());
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
