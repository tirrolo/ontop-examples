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
import it.unibz.krdb.sql.api.VisitedQuery;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;

public class InteractiveExample {

	/*
	 * Use the sample database using H2 from
	 * https://github.com/ontop/ontop/wiki/InstallingTutorialDatabases
	 * 
	 * Please use the pre-bundled H2 server from the above link
	 * 
	 */
	final String owlfile = "src/main/resources/davide/npd-v2-ql_a.owl";
	final String obdafile = "src/main/resources/davide/npd-v2-ql_a.obda";

	public void runQuery() throws Exception {

		Statistics.setLabel("GLOBAL");
		
		/*
		 * Load the ontology from an external .owl file.
		 */
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new File(owlfile));

		/*
		 * Load the OBDA model from an external .obda file
		 */
		OBDADataFactory fac = OBDADataFactoryImpl.getInstance();
		OBDAModel obdaModel = fac.getOBDAModel();
		ModelIOManager ioManager = new ModelIOManager(obdaModel);
		ioManager.load(obdafile);

		/*
		 * Prepare the configuration for the Quest instance. The example below shows the setup for
		 * "Virtual ABox" mode
		 */
		QuestPreferences preference = new QuestPreferences();
		preference.setCurrentValueOf(QuestPreferences.ABOX_MODE, QuestConstants.VIRTUAL);
//		preference.setCurrentValueOf(QuestPreferences.REFORMULATION_TECHNIQUE, QuestConstants.TW);
//		preference.setCurrentValueOf(QuestPreferences.REWRITE, QuestConstants.TRUE);

		/*
		 * Create the instance of Quest OWL reasoner.
		 */
		QuestOWLFactory factory = new QuestOWLFactory();
		factory.setOBDAController(obdaModel);
		factory.setPreferenceHolder(preference);
		QuestOWL reasoner = (QuestOWL) factory.createReasoner(ontology, new SimpleConfiguration());

		
		String outFile = "src/main/resources/davide/QueriesStdout/prova";
		
		
		/*
		 * Prepare the data connection for querying.
		 */
		QuestOWLConnection conn = reasoner.getConnection();

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		QuestOWLStatement st = conn.createStatement();
		while(true){
			System.out.println("INSERT A QUERY");
			try {
				StringBuilder builder = new StringBuilder();
				String curLine = null;
				while( !(curLine = br.readLine()).equals("!!") ){
					builder.append(curLine+"\n");
				}
				String sparqlQuery = builder.toString();
				System.out.println(sparqlQuery);
				System.out.println("INSERT A LABEL");
				String label = br.readLine();
				Statistics.setLabel(label);
				QuestOWLResultSet rs = st.executeTuple(sparqlQuery);
				int columnSize = rs.getColumCount();
				int cnt = 0;
				while (rs.nextRow()) {
					for (int idx = 1; idx <= columnSize; idx++) {
						OWLObject binding = rs.getOWLObject(idx);
//						System.out.print(binding.toString() + ", ");
					}
					cnt++;
					//System.out.print("\n");
				}
				Statistics.setInt("GLOBAL", "n_triples", cnt);
				rs.close();
				System.out.println("Results count: " + cnt);
				
				
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
			System.out.println("===========STATISTICS RECAP===========");
			System.out.println(Statistics.printStats());
		}	
	}
		
	/**
	 * Main client program
	 */
	public static void main(String[] args) {
		
		try {
			InteractiveExample example = new InteractiveExample();

				example.runQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
