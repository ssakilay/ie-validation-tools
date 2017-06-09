package ietools;

import java.sql.*;
import java.util.*;

public class FrameGenerator
{
	private Connection conn;
	private PreparedStatement slotTypeStmt;
	private PreparedStatement insertSlotStmt;
	private PreparedStatement valueSlotStmt;
	private PreparedStatement frameSlotStmt;
	private PreparedStatement updateCRFStmt;
	private PreparedStatement frameSectionStmt;
	private String schema = "validator.";
	
	public FrameGenerator()
	{
	}
	
	public FrameGenerator(Connection conn, String schema)
	{
		this.conn = conn;
		this.schema = schema;
	}
	
	public void genFrame(int crfID, Map<Integer, String> slotAnnotMap, Map<Integer, String> slotCondMap)
	{
		try {
			slotTypeStmt = conn.prepareStatement("select id from " + schema + "data_type where name = ?");
			insertSlotStmt = conn.prepareStatement("insert into " + schema + "slot (name, annotation_type, slot_type, cond) values (?,?,?,?)");
			valueSlotStmt = conn.prepareStatement("update " + schema + "value set slot_id = ? where value_id = ?");
			frameSlotStmt = conn.prepareStatement("insert into " + schema + "frame_slot (frame_id, slot_id) values (?,?)");
			frameSectionStmt = conn.prepareStatement("insert into " + schema + "frame_section (frame_id, section_id, slot_id, primary_key) values (?,?,?,?)");
			
			
			Statement stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("select name from " + schema + "crf where crf_id = " + crfID);
			String frameName = "";
			if (rs.next())
				frameName = rs.getString(1);
			
			stmt.execute("insert into " + schema + "frame (name) values ('" + frameName + "')");
			int frameID = getLastID();
			
			/*
			rs = stmt.executeQuery("select last_insert_id()");
			if (rs.next()) {
				frameID = rs.getInt(1);
			}
			*/
			
			stmt.execute("update " + schema + "crf set frame_id = " + frameID + " where crf_id = " + crfID);
			
			rs = stmt.executeQuery("select d.value_id, d.html_id, b.name, c.element_id "
				+ "from " + schema + "element a, " + schema + "data_type b, " + schema + "crf_element c, " + schema + "value d, " + schema + "element_value e "
				+ "where a.data_type = b.id and "
				+ "d.value_id = e.value_id and a.element_id = e.element_id and "
				+ "c.element_id = a.element_id and c.crf_id = " + crfID + " order by c.element_id");
			
			int index = 1;
			int lastElementID = -1;
			while (rs.next()) {
				int valueID = rs.getInt(1);
				String htmlID = rs.getString(2);
				String elementTypeName = rs.getString(3);
				int elementID = rs.getInt(4);
				
				String slotTypeName = "string";
				if (elementTypeName.equals("number"))
					slotTypeName = "number";
				
				int slotType = getSlotType(slotTypeName);
				String annotType = slotAnnotMap.get(elementID);
				String condition = slotCondMap.get(valueID);
				
				if (lastElementID == elementID) {
					annotType = annotType + "-" + index;
					index++;
				}
				else
					index = 1;
				
				int slotID = insertSlot(htmlID, annotType, slotType, condition);
				valueSlotStmt.setInt(1, slotID);
				valueSlotStmt.setInt(2, valueID);
				valueSlotStmt.execute();
				
				frameSlotStmt.setInt(1, frameID);
				frameSlotStmt.setInt(2, slotID);
				frameSlotStmt.execute();
				
				lastElementID = elementID;
			}
			
			stmt.close();
			slotTypeStmt.close();
			insertSlotStmt.close();
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private int insertSlot(String name, String annotType, int slotType, String condition) throws SQLException
	{
		insertSlotStmt.setString(1, name);
		insertSlotStmt.setString(2, annotType);
		insertSlotStmt.setInt(3, slotType);
		insertSlotStmt.setString(4, condition);
		insertSlotStmt.execute();
		
		int slotID = getLastID();
		
		/*
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select last_insert_id()");
		if (rs.next()) {
			slotID = rs.getInt(1);
		}
		
		stmt.close();
		*/
		
		return slotID;
	}
	
	private int getSlotType(String name) throws SQLException
	{
		int slotType = -1;
		slotTypeStmt.setString(1, name);
		ResultSet rs = slotTypeStmt.executeQuery();
		if (rs.next())
			slotType = rs.getInt(1);
		
		return slotType;
	}
	
	private int getLastID() throws SQLException
	{
		int lastIndex = -1;
		String dbType = conn.getMetaData().getDatabaseProductName();
		String queryStr = "select last_insert_id()";
		if (dbType.equals("Microsoft SQL Server"))
			//queryStr = "select scope_identity()";
			queryStr = "select @@identity";
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(queryStr);
		if (rs.next())
			lastIndex = rs.getInt(1);
		
		stmt.close();
		
		return lastIndex;
	}
}
