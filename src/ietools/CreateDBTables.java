package ietools;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

public class CreateDBTables
{
	private Connection conn;
	private String schema;
	private Properties props;
	
	private Gson gson;
	
	public CreateDBTables()
	{
		gson = new Gson();
	}
	
	public void init(String user, String password, String host, String dbName, String dbType, String config) throws SQLException, ClassNotFoundException, FileNotFoundException, IOException
	{
		conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		props = new Properties();
		props.load(new FileReader(config));
		schema = props.getProperty("schema");
	}
	
	public void close() throws SQLException
	{
		conn.close();
	}
	
	public void createDBTables() throws SQLException, IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(props.getProperty("sqlFileName")));
		String line = "";
		StringBuilder strBlder = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			line = line.replaceAll("SCHEMA\\.", schema + ".");
			strBlder.append(line + "\n");
		}
		
		System.out.println(strBlder.toString());
		
		String[] queries = strBlder.toString().split(";");
		
		Statement stmt = conn.createStatement();
		for (int i=0; i<queries.length; i++) {
			if (queries[i].trim().length() > 0) {
				System.out.println(queries[i]);
				stmt.execute(queries[i]);
			}
		}
		
		//stmt.executeBatch();
		
		
		stmt.close();
		
		reader.close();

	}
	
	
	public void createUsers() throws SQLException
	{
		List<String> userList = new ArrayList<String>();
		userList = gson.fromJson(props.getProperty("users"), userList.getClass());
		
		String rq = DBConnection.reservedQuote;
		PreparedStatement pstmt = conn.prepareStatement("insert into " + schema + "." + rq + "user" + rq + " (user_name, pw) values (?,?)");
		System.out.println("insert into " + schema + "." + rq + "user" + rq + " (user_name, pw) values (?,?)");
		
		for (String user : userList) {
			String[] parts = user.split("\\|");
			pstmt.setString(1, parts[0]);
			pstmt.setString(2, parts[1]);
			pstmt.execute();
		}
		
		pstmt.close();
	}
	
	public static void main(String[] args)
	{
		if (args.length != 6) {
			System.out.println("usage: user password host dbName dbType config");
			System.exit(0);
		}
		
		try {
			CreateDBTables create = new CreateDBTables();
			create.init(args[0], args[1], args[2], args[3], args[4], args[5]);
			create.createDBTables();
			create.createUsers();
			create.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
