package ietools;

import java.sql.*;

import utils.db.DBConnection;

public class DeleteProject
{
	private Connection conn;
	private String schema;
	
	public DeleteProject()
	{
	}
	
	public void setSchema(String schema)
	{
		this.schema = schema + ".";
	}
	
	public void init(String user, String password, String host, String dbName, String dbType)
	{
		try {
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		try {
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void deleteProject(String projectName)
	{
		try {
			PreparedStatement pstmtDeleteFrameInstance = conn.prepareStatement("delete from " + schema + "frame_instance where frame_instance_id = ?");
			PreparedStatement pstmtDeleteFrameInstanceDoc = conn.prepareStatement("delete from " + schema + "frame_instance_document where frame_instance_id = ?");
			PreparedStatement pstmtDeleteFrameInstanceData = conn.prepareStatement("delete from " + schema + "frame_instance_data where frame_instance_id = ?");
			PreparedStatement pstmtDeleteFrameInstanceDocHist = conn.prepareStatement("delete from " + schema + "frame_instance_document_history where frame_instance_id = ?");
			PreparedStatement pstmtDeleteFrameInstanceElem = conn.prepareStatement("delete from " + schema + "frame_instance_element_repeat where frame_instance_id = ?");
			PreparedStatement pstmtDeleteFrameInstanceSection = conn.prepareStatement("delete from " + schema + "frame_instance_section_repeat where frame_instance_id = ?");
			PreparedStatement pstmtDeleteFrameInstanceStatus = conn.prepareStatement("delete from " + schema + "frame_instance_status where frame_instance_id = ?");
			
			PreparedStatement pstmtDeleteCRF = conn.prepareStatement("delete from " + schema + "crf where crf_id = ?");
			PreparedStatement pstmtDeleteValue = conn.prepareStatement("delete from " + schema + "value where value_id in "
				+ "(select a.value_id from " + schema + "element_value a, " + schema + "crf_element b where a.element_id = b.element_id and "
				+ "b.crf_id = ?)");
			PreparedStatement pstmtDeleteElement = conn.prepareStatement("delete from " + schema + "element where element_id in "
				+ "(select a.element_id from " + schema + "crf_element a where a.crf_id = ?)");
			PreparedStatement pstmtDeleteElementValue = conn.prepareStatement("delete from " + schema + "element_value where "
				+ "element_id in (select a.element_id from " + schema + "crf_element a where a.crf_id = ?)");
			PreparedStatement pstmtDeleteCRFElement = conn.prepareStatement("delete from " + schema + "crf_element where crf_id = ?");
			PreparedStatement pstmtDeleteCRFSection = conn.prepareStatement("delete from " + schema + "crf_section where crf_id = ?");
			
			
			PreparedStatement pstmtDeleteProject = conn.prepareStatement("delete from " + schema + "project where project_id = ?");
			PreparedStatement pstmtDeleteCRFProj = conn.prepareStatement("delete from " + schema + "crf_project where project_id = ?");
			
			
			Statement stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("select project_id from " + schema + "project where name = '" + projectName + "'");
			int projID = -1;
			if (rs.next()) {
				projID = rs.getInt(1);
			}
			
			rs = stmt.executeQuery("select frame_instance_id from " + schema + "project_frame_instance where project_id = " + projID);
			while (rs.next()) {
				int frameInstanceID = rs.getInt(1);
				pstmtDeleteFrameInstance.setInt(1, frameInstanceID);
				pstmtDeleteFrameInstance.execute();
				
				pstmtDeleteFrameInstanceDoc.setInt(1, frameInstanceID);
				pstmtDeleteFrameInstanceDoc.execute();
				
				pstmtDeleteFrameInstanceData.setInt(1, frameInstanceID);
				pstmtDeleteFrameInstanceData.execute();
				
				pstmtDeleteFrameInstanceDocHist.setInt(1, frameInstanceID);
				pstmtDeleteFrameInstanceDocHist.execute();
				
				pstmtDeleteFrameInstanceElem.setInt(1, frameInstanceID);
				pstmtDeleteFrameInstanceElem.execute();
				
				pstmtDeleteFrameInstanceSection.setInt(1, frameInstanceID);
				pstmtDeleteFrameInstanceSection.execute();
				
				pstmtDeleteFrameInstanceStatus.setInt(1, frameInstanceID);
				pstmtDeleteFrameInstanceStatus.execute();
			}
				
			pstmtDeleteProject.setInt(1, projID);
			pstmtDeleteProject.execute();
			

			pstmtDeleteCRFProj.setInt(1, projID);
			pstmtDeleteCRFProj.execute();
			
			
			/*
			rs = stmt.executeQuery("select crf_id from " + schema + "crf where crf_id not in "
				+ "(select distinct a.crf_id from " + schema + "crf_project a)"); 
			
			while (rs.next()) {
				int crfID = rs.getInt(1);
				pstmtDeleteCRF.setInt(1, crfID);
				pstmtDeleteCRF.execute();
				
				pstmtDeleteElement.setInt(1, crfID);
				pstmtDeleteElement.execute();
				
				pstmtDeleteValue.setInt(1, crfID);
				pstmtDeleteValue.execute();
				
				pstmtDeleteElementValue.setInt(1, crfID);
				pstmtDeleteElementValue.execute();
				
				pstmtDeleteCRFElement.setInt(1, crfID);
				pstmtDeleteCRFElement.execute();
			}
			*/
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void deleteFrame(int frameID)
	{
		try {
			Statement stmt = conn.createStatement();
			
			PreparedStatement pstmtDeleteCRF = conn.prepareStatement("delete from " + schema + "crf where crf_id = ?");
			PreparedStatement pstmtDeleteValue = conn.prepareStatement("delete from " + schema + "value where value_id in "
				+ "(select a.value_id from " + schema + "element_value a, " + schema + "crf_element b where a.element_id = b.element_id and "
				+ "b.crf_id = ?)");
			PreparedStatement pstmtDeleteElement = conn.prepareStatement("delete from " + schema + "element where element_id in "
				+ "(select a.element_id from " + schema + "crf_element a where a.crf_id = ?)");
			PreparedStatement pstmtDeleteElementValue = conn.prepareStatement("delete from " + schema + "element_value where "
				+ "element_id in (select a.element_id from " + schema + "crf_element a where a.crf_id = ?)");
			PreparedStatement pstmtDeleteCRFElement = conn.prepareStatement("delete from " + schema + "crf_element where crf_id = ?");
			PreparedStatement pstmtDeleteCRFSection = conn.prepareStatement("delete from " + schema + "crf_section where crf_id = ?");

				
			
			ResultSet rs = stmt.executeQuery("select crf_id from " + schema + "crf where frame_id = " + frameID); 
				
			while (rs.next()) {
				int crfID = rs.getInt(1);
				pstmtDeleteCRF.setInt(1, crfID);
				pstmtDeleteCRF.execute();
				
				pstmtDeleteElement.setInt(1, crfID);
				pstmtDeleteElement.execute();
				
				pstmtDeleteValue.setInt(1, crfID);
				pstmtDeleteValue.execute();
				
				pstmtDeleteElementValue.setInt(1, crfID);
				pstmtDeleteElementValue.execute();
				
				pstmtDeleteCRFElement.setInt(1, crfID);
			}
			
			
			stmt.execute("delete from " + schema + "frame where frame_id = " + frameID);
			
			stmt.execute("delete from " + schema + "slot where slot_id in "
				+ "(select a.slot_id from " + schema + "frame_slot a where a.frame_id = ?)");

			stmt.execute("delete from " + schema + "frame_slot where frame_id = " + frameID);
			stmt.execute("delete from " + schema + "frame_section where frame_id = " + frameID);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		if (args.length != 8) {
			System.out.println("usage: user password host dbName dbType schema project/frame projName/frameID");
			System.exit(0);
		}
		
		DeleteProject delete = new DeleteProject();
		delete.setSchema(args[5]);
		delete.init(args[0], args[1], args[2], args[3], args[4]);
		if (args[6].equals("project"))
			delete.deleteProject(args[7]);
		else
			delete.deleteFrame(Integer.parseInt(args[7]));
	}
}
