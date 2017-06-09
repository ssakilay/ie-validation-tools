package ietools;

import java.sql.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

public class ProjectGenerator
{
	private Connection conn;
	private Connection docConn;
	private String schema = "validator.";
	private String docTable;
	private List<String> docColumns;
	private List<String> entityColumns;
	private String orderbyColumn;
	private Gson gson;
	private String docNamespace;
	private String docTextColumn;
	private String docIDColumn;
	private String docQuery;
	private List<String> entityDelimiters;
	private List<String> docDelimiters;
	private String filterColumn;
	private List<Pattern> regExList;
	private List<Pattern> negExList;
	private boolean write = false;
	
	
	public ProjectGenerator()
	{
		gson = new Gson();
	}
	
	public void genProject(String user, String password, String docUser, String docPassword, String config)
	{
		try {
			/*
			String projName = "LDCT CRFs";
			String crfName = "LDCT";
			String docNamespace = "";
			*/
			
			//read from property files
			
			Properties props = new Properties();
			props.load(new FileReader(config));
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			
			String docHost = props.getProperty("docHost");
			String docDBName = props.getProperty("docDBName");
			String docDBType = props.getProperty("docDBType");
			
			String crfName = props.getProperty("crfName");
			String projName = props.getProperty("projName");
			
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			docIDColumn = props.getProperty("docIDColumn");
			docTextColumn = props.getProperty("docTextColumn");
			
			String docColumnsStr = props.getProperty("docColumns");
			String entityColumnsStr = props.getProperty("entityColumns");
			
			docColumns = new ArrayList<String>();
			docColumns = gson.fromJson(docColumnsStr, docColumns.getClass());
			
			entityColumns = new ArrayList<String>();
			entityColumns = gson.fromJson(entityColumnsStr, entityColumns.getClass());
			
			String entityDelimitersStr = props.getProperty("entityDelimiters");
			entityDelimiters = new ArrayList<String>();
			entityDelimiters = gson.fromJson(entityDelimitersStr, entityDelimiters.getClass());
			
			String docDelimitersStr = props.getProperty("docDelimiters");
			docDelimiters = new ArrayList<String>();
			docDelimiters = gson.fromJson(docDelimitersStr, docDelimiters.getClass());
			
			//this is for filtering in using regular expressions
			filterColumn = props.getProperty("filterColumn");
			List<String> filterRegExList = new ArrayList<String>();
			filterRegExList = gson.fromJson(props.getProperty("filterRegExList"), filterRegExList.getClass());
			
			if (filterRegExList != null) {
				regExList = new ArrayList<Pattern>();
				
				for (String regExStr : filterRegExList) {
					Pattern patt = Pattern.compile(regExStr);
					regExList.add(patt);
				}
			}
			
			//this is for filtering out using regular expressions
			List<String> filterNegExList = new ArrayList<String>();
			filterNegExList = gson.fromJson(props.getProperty("filterNegExList"), filterNegExList.getClass());
			negExList = new ArrayList<Pattern>();
			
			if (filterNegExList != null) {
				for (String negExStr : filterNegExList) {
					Pattern patt = Pattern.compile(negExStr);
					negExList.add(patt);
				}
			}
			
			//query to get the documents and their metadata
			docQuery = props.getProperty("docQuery");
			
			orderbyColumn = props.getProperty("orderbyColumn");
			
			write = Boolean.parseBoolean(props.getProperty("write"));
			
			//database schema
			schema = props.getProperty("schema") + ".";
			
			
			//connection to the internal database
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			
			//connection to the document database
			docConn = DBConnection.dbConnection(docUser, docPassword, docHost, docDBName, docDBType);
			
			Statement stmt = conn.createStatement();
			int crfID = -1;
			int frameID = -1;
			ResultSet rs = stmt.executeQuery("select crf_id, frame_id from " + schema + "crf where name = '" + crfName + "'");
			if (rs.next()) {
				crfID = rs.getInt(1);
				frameID = rs.getInt(1);
			}
			
			int projID = -1;
			if (write) {
				stmt.execute("insert into " + schema + "project (name) values ('" + projName + "')");
				projID = getLastID();
				stmt.execute("insert into " + schema + "crf_project (crf_id, project_id) values (" + crfID + "," + projID + ")");
			}
			
			
			List<Map<String, Object>> frameInstanceInfoList = getFrameInstanceList();
			
			//insert frame instances into frame instance table
			PreparedStatement pstmt = conn.prepareStatement("insert into " + schema + "frame_instance (name, frame_id) values (?,?)");
			
			//insert documents into frame instance document table
			PreparedStatement pstmt2 = conn.prepareStatement("insert into " + schema + "frame_instance_document (frame_instance_id, document_id, "
				+ "document_table, document_namespace, document_key, document_text_column, document_name, document_order, document_features) "
				+ "values (?,?,?,?,?,?,?,?,?)");
			
			//query to insert new project
			PreparedStatement pstmt3 = conn.prepareStatement("insert into " + schema + "project_frame_instance (project_id, frame_instance_id) values (?,?)");
			
			pstmt.setInt(2, frameID);
			
			pstmt2.setString(3, docTable);
			pstmt2.setString(4, docNamespace);
			pstmt2.setString(5, docIDColumn);
			pstmt2.setString(6, docTextColumn);
			

			//write frame instances into the database
			for (Map<String, Object> frameMap : frameInstanceInfoList) {
				List<Long> docList = (List<Long>) frameMap.get("docList");
				List<String> docNameList = (List<String>) frameMap.get("docNameList");
				List<Map<String, String>> docFeaturesList = (List<Map<String, String>>) frameMap.get("docFeaturesList");
				
				pstmt.setString(1, (String) frameMap.get("entityID"));
				
				if (write)
					pstmt.execute();
				
				int frameInstanceID = getLastID();
				System.out.println("frameInstanceID: " + frameInstanceID);
				
				pstmt2.setInt(1, frameInstanceID);
				for (int i=0; i<docList.size(); i++) {
					long docID = docList.get(i);
					String docName = docNameList.get(i);
					Map<String, String> docFeaturesMap = docFeaturesList.get(i);
					String docFeaturesStr = gson.toJson(docFeaturesMap);
					
					pstmt2.setLong(2, docID);
					pstmt2.setString(7, docName);
					pstmt2.setInt(8, i);
					pstmt2.setString(9, docFeaturesStr);
					
					if (write)
						pstmt2.execute();
				}
				
				pstmt3.setInt(1, projID);
				pstmt3.setInt(2, frameInstanceID);
				
				if (write)	
				pstmt3.execute();
			}
			
			
			conn.close();
			docConn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//get the last frame instance id
	private int getLastID() throws SQLException
	{
		int lastIndex = -1;
		String dbType = conn.getMetaData().getDatabaseProductName();
		String queryStr = "select last_insert_id()";
		if (dbType.equals("Microsoft SQL Server"))
			queryStr = "select @@identity";
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(queryStr);
		if (rs.next())
			lastIndex = rs.getInt(1);
		
		stmt.close();
		
		return lastIndex;
	}
	
	//get frame instances
	private List<Map<String, Object>> getFrameInstanceList() throws SQLException
	{
		List<Map<String, Object>> frameInstanceInfoList = new ArrayList<Map<String, Object>>();
		Map<String, Map<String, Object>> frameInstanceInfoMap = new HashMap<String, Map<String, Object>>();
		Statement stmt = docConn.createStatement();
		
		
		//retrieve document data from database using doc query
		ResultSet rs = stmt.executeQuery(docQuery);
		while (rs.next()) {
			
			String docText = rs.getString(docTextColumn);
			if (docText == null || docText.length() == 0)
				continue;
			
			if (filterColumn != null) {
				String filterValue = rs.getString(filterColumn);
				boolean flag = false;
				for (Pattern p : regExList) {
					Matcher m = p.matcher(filterValue);
					boolean flag2 = m.find();
					if (flag2) {
						flag = true;
						break;
					}
				}
				
				for (Pattern p : negExList) {
					Matcher m = p.matcher(filterValue);
					boolean flag2 = m.find();
					if (flag2) {
						flag = false;
						break;
					}
				}
				
				if (!flag)
					continue;
			}
			


			//build entityID
			//entity columns determine which columns to group by for each entity
			StringBuilder entityID = new StringBuilder();
			for (int i=0; i<entityColumns.size(); i++) {
				String entityCol = entityColumns.get(i);
				String result = rs.getString(entityCol);
				
				if (i > 0)
					entityID.append(entityDelimiters.get(i-1));
				
				entityID.append(result);
			}
			
			
			Map<String, Object> frameMap = frameInstanceInfoMap.get(entityID.toString());
			if (frameMap == null) {
				frameMap = new HashMap<String, Object>();
				frameMap.put("entityID", entityID.toString());
				frameInstanceInfoMap.put(entityID.toString(), frameMap);
				frameInstanceInfoList.add(frameMap);
			}
			
					
			long docID = rs.getLong(docIDColumn);
			
			Map<String, String> docFeaturesMap = new HashMap<String, String>();
			StringBuilder docName = new StringBuilder();
			for (int i=0; i<docColumns.size(); i++) {
				String colName = docColumns.get(i);
				String result = rs.getString(colName);
				docFeaturesMap.put(colName, result);
				if (i > 0)
					docName.append(docDelimiters.get(i-1));
				docName.append(result);
			}
			
			List<Long> docList = (List<Long>) frameMap.get("docList");
			if (docList == null) {
				docList = new ArrayList<Long>();
				frameMap.put("docList", docList);
			}
			
			List<String> docNameList = (List<String>) frameMap.get("docNameList");
			if (docNameList == null) {
				docNameList = new ArrayList<String>();
				frameMap.put("docNameList", docNameList);
			}
			
			List<Map<String, String>> docFeaturesList = (List<Map<String, String>>) frameMap.get("docFeaturesList");
			if (docFeaturesList == null) {
				docFeaturesList = new ArrayList<Map<String, String>>();
				frameMap.put("docFeaturesList", docFeaturesList);
			}
			
			docList.add(docID);
			docNameList.add(docName.toString());
			docFeaturesList.add(docFeaturesMap);
			
			System.out.println("DocID: " + docID + " Name: " + docName.toString());
		}
		
		stmt.close();
		
		return frameInstanceInfoList;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 5) {
			System.out.println("usage: user password docUser docPassword config");
			System.exit(0);
		}
		
		ProjectGenerator gen = new ProjectGenerator();
		//gen.genProject("fmeng", "fmeng", "192.99.99.99", "lung_cancer_screening", "mysql");
		gen.genProject(args[0], args[1], args[2], args[3], args[4]);
	}
}
