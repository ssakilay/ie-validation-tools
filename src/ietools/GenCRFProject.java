package ietools;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

public class GenCRFProject
{

	private Connection conn;
	private Gson gson;
	private PreparedStatement pstmtGetCRFID;
	private PreparedStatement pstmtInsertProj;
	private PreparedStatement pstmtInsertCRFProj;
	private PreparedStatement pstmtProjExists;
	
	private CRFProcessor crfProc;
	private Map<String, Object> map;
	private String schema;
	

	public GenCRFProject()
	{
		gson = new Gson();
	}
	
	
	public void init(String user, String password, String host, String dbName, String dbType, String jsonFile) throws SQLException, ClassNotFoundException, FileNotFoundException, IOException
	{
		conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		
		BufferedReader reader = new BufferedReader(new FileReader(jsonFile));
		map = new HashMap<String, Object>();
		map = gson.fromJson(reader, map.getClass());
		
		schema = (String) map.get("schema") + ".";
		
		pstmtGetCRFID = conn.prepareStatement("select crf_id from " + schema + "crf where name = ?");
		pstmtInsertProj = conn.prepareStatement("insert into " + schema + "project (name) values (?)");
		pstmtInsertCRFProj = conn.prepareStatement("insert into " + schema + "crf_project (project_id, crf_id) values (?,?)");
		
		pstmtProjExists = conn.prepareStatement("select count(*) from " + schema + "project where name = ?");
		
		reader.close();
	}
	
	
	public void close() throws SQLException
	{
		conn.close();
	}

	
	public void gen() throws SQLException, IOException
	{
		
		
		//process CRFs
		crfProc = new CRFProcessor(schema);
		crfProc.setConnection(conn);
		
		List<String> crfFiles = new ArrayList<String>();
		crfFiles =  (List<String>) map.get("crfs");
		
		for (String crfFile : crfFiles) {
			crfProc.readCRFFile(crfFile);
		}
		
		
		//create projects
		List<Map<String, String>> projList = (List<Map<String, String>>) map.get("projects");
		
		for (Map<String, String> projMap : projList) {
			String projName = projMap.get("projName");
			String crfName = projMap.get("crfName");
			
			int projCount = 0;
			pstmtProjExists.setString(1, projName);
			ResultSet rs = pstmtProjExists.executeQuery();
			if (rs.next()) {
				projCount = rs.getInt(1);
			}
			
			if (projCount > 0)
				continue;
			
			int crfID = getCRFID(crfName);
			
			pstmtInsertProj.setString(1, projName);
			pstmtInsertProj.execute();
			
			int projID = getLastID();
			pstmtInsertCRFProj.setInt(1, projID);
			pstmtInsertCRFProj.setInt(2, crfID);
			pstmtInsertCRFProj.execute();
		}
	}
	
	
	private int getCRFID(String crfName) throws SQLException
	{
		int crfID = -1;
		pstmtGetCRFID.setString(1, crfName);
		ResultSet rs = pstmtGetCRFID.executeQuery();
		if (rs.next()) {
			crfID = rs.getInt(1);
		}
		
		return crfID;
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
	
	
	public static void main(String[] args)
	{
		if (args.length != 6) {
			System.out.println("usage: user password host dbName dbType config");
			System.exit(0);
		}
		
		try {
			GenCRFProject gen = new GenCRFProject();
			gen.init(args[0], args[1], args[2], args[3], args[4], args[5]);
			gen.gen();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
