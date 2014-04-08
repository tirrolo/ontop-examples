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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;

import connection.DBMSConnection;
import basicDatatypes.QualifiedName;
import basicDatatypes.Template;

public class QueryMixer {
	
	private String jdbcConnector = "jdbc:mysql";
	private static String dbUrl = "10.7.20.39:3306/npd";
	private String username = "test";
	private String passw = "ontop2014";
	
	public static boolean twRewriting = false;
	
	public static String owlfile = "src/main/resources/davide/npd-v2-ql_a.owl";
	public static String obdafile = "src/main/resources/davide/npd-v2-ql_a.obda";
	
	private Map<String, Integer> resultSetPointer = new HashMap<String, Integer>();

	DBMSConnection db = new DBMSConnection(jdbcConnector, dbUrl, username, passw);
	
	public void runQueryMixesTest(int nMixes) throws Exception {
		
		Statistics.expensiveStatsOff();

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
	
		if( twRewriting ){
			preference.setCurrentValueOf(QuestPreferences.REFORMULATION_TECHNIQUE, QuestConstants.TW);
			preference.setCurrentValueOf(QuestPreferences.REWRITE, QuestConstants.TRUE);
		}
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
		
		File folder = new File("src/main/resources/davide/Templates");
		File[] listOfFiles = folder.listFiles();
		for( int i = 0; i < listOfFiles.length; ++i ){
			System.out.println(listOfFiles[i]);
		}
		
		String statsFileName = "src/main/resources/davide/statsMixer.txt";
		File statsFile = new File(statsFileName);
		if( statsFile.exists() ) statsFile.delete();
		FileWriter statsWriter = new FileWriter(statsFile);
		
		
		for( int j = 0; j < nMixes; ++j ){
			
			long start = System.currentTimeMillis();
			long wastedTime = 0;
			for( int i = 0; i < listOfFiles.length; ++i ){
				
				long wasteStart = System.currentTimeMillis();
				
				if( !listOfFiles[i].isFile() ) continue;
				
				String inFile = "src/main/resources/davide/Templates/" + listOfFiles[i].getName();
				String confFile = "src/main/resources/davide/TemplatesConf/" + listOfFiles[i].getName();
				BufferedReader in = new BufferedReader(new FileReader(inFile));
				
				StringBuilder queryBuilder = new StringBuilder();
				String curLine = null;
				
				while( (curLine = in.readLine()) != null ){
					queryBuilder.append(curLine + "\n");
				}
				in.close();
				Statistics.setLabel(listOfFiles[i].getName()+"_mix_"+j);
				Template sparqlQueryTemplate = new Template(queryBuilder.toString(), "$");
				
				// Get the placeholders
				in = new BufferedReader(new FileReader(confFile));
				
				List<QualifiedName> qNames = new ArrayList<QualifiedName>();
				while( (curLine = in.readLine()) != null ){
					qNames.add(new QualifiedName(curLine));
				}
				
				in.close();
				
				// Find a mix
				fillPlaceholders(sparqlQueryTemplate, qNames);
				wastedTime += System.currentTimeMillis() - wasteStart;
				try {				
					long queryStart = System.currentTimeMillis();
					QuestOWLResultSet rs = st.executeTuple(sparqlQueryTemplate.getFilled());
					long queryEnd = System.currentTimeMillis();
					Statistics.setTime(Statistics.getLabel(), "query_execution_time", queryEnd - queryStart);
					int columnSize = rs.getColumCount();
					
					queryStart = System.currentTimeMillis();
					int resultsCount = 0;
					while (rs.nextRow()) {
						for (int idx = 1; idx <= columnSize; idx++) {
							OWLObject binding = rs.getOWLObject(idx);
							++resultsCount;
							System.out.print(binding.toString() + ", ");
						}
						System.out.print("\n");
					}
					queryEnd = System.currentTimeMillis();
					Statistics.setTime(Statistics.getLabel(), "query_display_time", queryEnd - queryStart);
					Statistics.setInt(Statistics.getLabel(), "query_num_results", resultsCount);
					rs.close();
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			
			long end = System.currentTimeMillis() - wastedTime;
			
			Statistics.setTime("GLOBAL", "mix_time_"+j, end - start);	
		}
		statsWriter.write(Statistics.printStats());
		statsWriter.flush();
		statsWriter.close();
	}

	private void fillPlaceholders(Template sparqlQueryTemplate,
			List<QualifiedName> qNames) {
		
		if(sparqlQueryTemplate.getNumPlaceholders() == 0) return;
		
		List<String> fillers = new ArrayList<String>();
		
		for(QualifiedName qN : qNames ){
			
			int pointer = 0;
			if( resultSetPointer.containsKey(qN.toString()) ){
				pointer = resultSetPointer.get(qN.toString());
				resultSetPointer.put(qN.toString(), pointer + 1);
			}
			else{
				resultSetPointer.put(qN.toString(), 1);
			}
			
			String query = "SELECT DISTINCT " + qN.getColName() + " FROM " 
					+ qN.getTableName() + " LIMIT " + pointer+ ", 1";
			
			PreparedStatement stmt = db.getPreparedStatement(query);
			
			try {
				ResultSet rs = stmt.executeQuery();
			
				if ( !rs.next() ){
					stmt.close();
					query = "SELECT DISTINCT " + qN.getColName() + " FROM " 
							+ qN.getTableName() + " LIMIT " + 0 + ", 1";
					resultSetPointer.put(qN.toString(), 1);
					
					stmt = db.getPreparedStatement(query);
					
					rs = stmt.executeQuery();
					if( !rs.next() ){
						System.err.println("Problem");
					}
				}
				fillers.add( rs.getString(qN.getColName()) );
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		for( int i = 1; i <= fillers.size(); ++i ){
			sparqlQueryTemplate.setNthPlaceholder(i, fillers.get(i-1));
		}
	}
	
	public static void setDbUrl(String dbUrl){
		QueryMixer.dbUrl = dbUrl;
	}
	

	/**
	 * Main client program
	 */
	public static void main(String[] args) {
		
		if( args.length != 5 ){
			System.out.println("Usage: command dbName nMixes owlFile obdaFile twOn");
			System.exit(1);
		}
		
		System.out.println(args);
		
		String dbName = args[0];
		int nMixes = Integer.parseInt(args[1]);
		
		String url = "10.7.20.39:3306/";
		String dbUrl = url + dbName;
		
		QueryMixer.setDbUrl(dbUrl);
		
		owlfile = args[2];
		obdafile = args[3];
		
		if(Boolean.parseBoolean(args[4]) == true){
			twRewriting = true;
		}
		try {
			QueryMixer qM = new QueryMixer();
			
			qM.runQueryMixesTest(nMixes);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
