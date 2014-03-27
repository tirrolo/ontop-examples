package org.semanticweb.ontop.examples;

import it.unibz.krdb.obda.io.ModelIOManager;
import it.unibz.krdb.obda.model.OBDADataFactory;
import it.unibz.krdb.obda.model.OBDAModel;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.owlrefplatform.core.QuestConstants;
import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import it.unibz.krdb.obda.owlrefplatform.dav.utils.Statistics;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWL;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLConnection;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLFactory;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLResultSet;
import it.unibz.krdb.obda.owlrefplatform.owlapi3.QuestOWLStatement;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;

import ch.qos.logback.classic.Logger;

public class DavideExample {

	/*
	 * Use the sample database using H2 from
	 * https://github.com/ontop/ontop/wiki/InstallingTutorialDatabases
	 * 
	 * Please use the pre-bundled H2 server from the above link
	 * 
	 */
	final String owlfile = "src/main/resources/davide/npd-v2-materialized.owl";
	final String obdafile = "src/main/resources/davide/npd-v2-materialized.obda";

	public void runQuery() throws Exception {

		/*
		 * Load the ontology from an external .owl file.
		 */
		long totalStartTime = System.currentTimeMillis();
		long startTime = totalStartTime;
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new File(owlfile));
		long endTime = System.currentTimeMillis();
		
		Statistics.setLabel("GLOBAL");
		Statistics.addTime(Statistics.getLabel(), "loadOwlOntologyTime", endTime - startTime);

		/*
		 * Load the OBDA model from an external .obda file
		 */
		OBDADataFactory fac = OBDADataFactoryImpl.getInstance();
		OBDAModel obdaModel = fac.getOBDAModel();
		ModelIOManager ioManager = new ModelIOManager(obdaModel);
		startTime = System.currentTimeMillis();
		ioManager.load(obdafile);
		endTime = System.currentTimeMillis() - Statistics.getWastedTime();
		Statistics.addTime(Statistics.getLabel(), "loadOBDAFileTime", endTime - startTime);

		/*
		 * Prepare the configuration for the Quest instance. The example below shows the setup for
		 * "Virtual ABox" mode
		 */
		QuestPreferences preference = new QuestPreferences();
		preference.setCurrentValueOf(QuestPreferences.ABOX_MODE, QuestConstants.VIRTUAL);
		preference.setCurrentValueOf(QuestPreferences.REFORMULATION_TECHNIQUE, QuestConstants.TW);
		preference.setCurrentValueOf(QuestPreferences.REWRITE, QuestConstants.TRUE);

		/*
		 * Create the instance of Quest OWL reasoner.
		 */
		QuestOWLFactory factory = new QuestOWLFactory();
		factory.setOBDAController(obdaModel);
		factory.setPreferenceHolder(preference);
		startTime = System.currentTimeMillis();
		QuestOWL reasoner = (QuestOWL) factory.createReasoner(ontology, new SimpleConfiguration());
		endTime = System.currentTimeMillis() - Statistics.getWastedTime();
		Statistics.addTime(Statistics.getLabel(), "classification_time", endTime - startTime);
		
		// Statistics
//		reasoner.
		//
		
		/*
		 * Prepare the data connection for querying.
		 */
		QuestOWLConnection conn = reasoner.getConnection();
		QuestOWLStatement st = conn.createStatement();
		
		Statistics.addTime(Statistics.getLabel(), "total_init_time", System.currentTimeMillis() - Statistics.getWastedTime() - totalStartTime);
		
		File folder = new File("src/main/resources/davide/Queries");
		File[] listOfFiles = folder.listFiles();
		for( int i = 0; i < listOfFiles.length; ++i ){
			System.out.println(listOfFiles[i]);
		}
		
		PrintStream console = System.out;
		
		String statsFileName = "src/main/resources/davide/stats.txt";
		File statsFile = new File(statsFileName);
		if( statsFile.exists() ) statsFile.delete();
		FileWriter statsWriter = new FileWriter(statsFile);
		for( int i = 0; i < listOfFiles.length; ++i ){
			
			if( !listOfFiles[i].isFile() ) continue;
			
			String inFile = "src/main/resources/davide/Queries/" + listOfFiles[i].getName();
			String outFile = "src/main/resources/davide/QueriesStdout/" + listOfFiles[i].getName();
			String errFile = "src/main/resources/davide/QueriesStderr/" + listOfFiles[i].getName();
			System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream(outFile))));
			System.setErr(new PrintStream(new BufferedOutputStream(new FileOutputStream(errFile))));
			BufferedReader in = new BufferedReader(new FileReader(inFile));
	
			
			StringBuilder queryBuilder = new StringBuilder();
			String curLine = null;
			
			while( (curLine = in.readLine()) != null ){
				queryBuilder.append(curLine + "\n");
			}
			Statistics.setLabel(listOfFiles[i].getName());
			String sparqlQuery = queryBuilder.toString();
			try {				
				System.out.println(sparqlQuery);
				QuestOWLResultSet rs = st.executeTuple(sparqlQuery);
				int columnSize = rs.getColumCount();
				while (rs.nextRow()) {
					for (int idx = 1; idx <= columnSize; idx++) {
						OWLObject binding = rs.getOWLObject(idx);
						System.out.print(binding.toString() + ", ");
					}
					System.out.print("\n");
				}
				rs.close();
				
				/*
				 * Print the query summary
				 */
				QuestOWLStatement qst = (QuestOWLStatement) st;
				String sqlQuery = qst.getUnfolding(sparqlQuery);
				
				System.out.println();
				System.out.println("The input SPARQL query:");
				System.out.println("=======================");
				System.out.println(sparqlQuery);
				System.out.println();
				
				System.out.println("The output SQL query:");
				System.out.println("=====================");
				System.out.println(sqlQuery);				
			}
			catch(Exception e){
				e.printStackTrace();
			}
			in.close();
		}	
		System.setOut(console);
		System.out.println(Statistics.printStats());
		statsWriter.write(Statistics.printStats());
		statsWriter.flush();
		statsWriter.close();
	}

	/**
	 * Main client program
	 */
	public static void main(String[] args) {
		
			DavideExample example = new DavideExample();				
			try {
				example.runQuery();
			} catch (Exception e) {
				e.printStackTrace();
			}		
	}
	
}
