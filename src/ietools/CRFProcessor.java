package ietools;

import java.io.*;
import java.sql.*;
import java.util.*;

//import play.db.DB;
import utils.db.DBConnection;

import com.google.gson.Gson;

public class CRFProcessor
{
	private Gson gson;
	private Connection conn;
	//private List<Map<String, Object>> sectionList;
	private List<Map<String, Object>> dataList;
	private String crfName;
	private FrameGenerator frameGen;
	private Map<Integer, String> slotAnnotMap;
	private Map<Integer, String> slotCondMap;
	private String schema = "validator.";
	private String rq;

	public CRFProcessor(String schema)
	{
		gson = new Gson();
		this.schema = schema;
	}
	
	public String readCRFDB(int crfID, List<Map<String, Object>> sectionList) throws SQLException
	{
		getConnection();
		//sectionList = (List<Map<String, Object>>) Cache.get("sectionList");
		//schema = (String) Cache.get("schemaName");
		rq = getReservedQuote();
		
		List<Map<String, Object>> dataList = readCRFFromDB(crfID, sectionList, -1);
		
		//Cache.set("dataList", dataList);
		//Cache.set("sectionList", sectionList);
		//Cache.set("crfID", crfID);

		String jsonStr = gson.toJson(dataList);
		//System.out.println(jsonStr);
		
		conn.close();
		
		return jsonStr;
	}
	
	public String readCRFDB(int crfID, List<Map<String, Object>> sectionList, int frameInstanceID) throws SQLException
	{
		getConnection();
		//sectionList = (List<Map<String, Object>>) Cache.get("sectionList");
		//schema = (String) Cache.get("schemaName");
		rq = getReservedQuote();
		
		List<Map<String, Object>> dataList = readCRFFromDB(crfID, sectionList, frameInstanceID);
		
		//Cache.set("dataList", dataList);
		//Cache.set("sectionList", sectionList);
		//Cache.set("crfID", crfID);

		String jsonStr = gson.toJson(dataList);
		//System.out.println(jsonStr);
		
		conn.close();
		
		return jsonStr;
	}
	
	public String readCRFFile(String user, String password, String host, String dbName, String dbType, String fileName) throws IOException, SQLException, ClassNotFoundException
	{
		conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		frameGen = new FrameGenerator(conn, schema);
		slotAnnotMap = new HashMap<Integer, String>();
		slotCondMap = new HashMap<Integer, String>();
		
		StringBuilder strBlder = new StringBuilder();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line = "";
		while ((line = reader.readLine()) != null) {
			strBlder.append(line + "\n");
		}
		
		//System.out.println(strBlder.toString());
		
		dataList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> sectionList = new ArrayList<Map<String, Object>>();
		
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap = gson.fromJson(strBlder.toString(), dataMap.getClass());
		
		String crfName = (String) dataMap.get("name");
		
		//List<Map<String, Object>> elementList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> elementList = (List<Map<String, Object>>) dataMap.get("elements");

		for (int i=0; i<elementList.size(); i++) {
			Map<String, Object> map = elementList.get(i);
			String type = (String) map.get("type");
			
			if (type.equals("section")) {
				Map<String, Object> sectionMap = new HashMap<String, Object>();
				String sectionName = (String) map.get("display");
				
				Integer repeat = 0;
				Double repeatDouble = (Double) map.get("repeat");
				if (repeatDouble != null)
					repeat = repeatDouble.intValue();

				sectionMap.put("name", sectionName);
				sectionMap.put("repeat", repeat);
				sectionMap.put("startIndex", dataList.size());
				sectionList.add(sectionMap);
				
				List<Map<String, Object>> sectionElementList = (List<Map<String, Object>>) map.get("elements");				
				List<Map<String, Object>> htmlElements = getElementDefs(sectionElementList, sectionName);
				sectionMap.put("elements", htmlElements);
				dataList.addAll(htmlElements);
			}
			else {
				dataList.add(getElementDef(map, "default"));
			}

		}
		
		int crfID = writeCRFToDB(crfName, sectionList, dataList);
		
		frameGen.genFrame(crfID, slotAnnotMap, slotCondMap);
		
		
		String jsonStr = gson.toJson(dataList);
		System.out.println(jsonStr);
		
		reader.close();
		conn.close();
		
		return jsonStr;
	}
	
	public String addRemoveSection(int crfID, int frameInstanceID, String sectionName, int increment, List<Map<String, Object>> sectionList) throws SQLException
	{
		getConnection();
		
		//schema = (String) Cache.get("schemaName");

		
		//sectionList = (List<Map<String, Object>>) Cache.get("sectionList");
		//int crfID = (Integer) Cache.get("crfID");
		
		for (Map<String, Object> sectionMap : sectionList) {
			String sectionName2 = (String) sectionMap.get("name");
			System.out.println("addremove: " + sectionName + ", " + sectionName2);
			if (sectionName2.equals(sectionName)) {
				int repeatNum = ((Number) sectionMap.get("repeatNumber")).intValue();
				repeatNum += increment;
				
				if (repeatNum < 0)
					repeatNum = 0;
				
				sectionMap.put("repeatNumber", repeatNum);
				int sectionID = ((Number) sectionMap.get("sectionID")).intValue();
				
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select count(*) from " + schema + "frame_instance_section_repeat where frame_instance_id = " + frameInstanceID + " and "
					+ "section_id = " + sectionID);
				
				boolean update = false;
				if (rs.next()) {
					if (rs.getInt(1) > 0)
						update = true;
				}
				
				if (!update) {
					stmt.execute("insert into " + schema + "frame_instance_section_repeat (frame_instance_id, section_id, repeat_num) values ('" + frameInstanceID + "', "
						+ sectionID + "," + repeatNum + ")");
				}
				else {
					stmt.execute("update " + schema + "frame_instance_section_repeat set repeat_num = " + repeatNum + " where frame_instance_id = " + frameInstanceID + " and "
						+ "section_id = " + sectionID);
				}
				
				stmt.close();
			}
		}
		
		conn.close();
	
		return readCRFDB(crfID, sectionList, frameInstanceID);
	}
	
	public String addRemoveElement(int crfID, int frameInstanceID, String htmlID, int increment, List<Map<String, Object>> sectionList) throws SQLException
	{
		getConnection();
		
		int index = htmlID.lastIndexOf("_");
		int index2 = htmlID.substring(0, index).lastIndexOf("_");
		
		String rootHTMLID = htmlID.substring(0, index2);
		
		int sectionSlotNum = Integer.parseInt(htmlID.substring(index2+1, index));
		
		//get elementID
		int elementID = -1;
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select element_id from " + schema + "element where html_id = '" + rootHTMLID + "'");
		if (rs.next()) {
			elementID = rs.getInt(1);
		}
		
		rs = stmt.executeQuery("select repeat_num from " + schema + "frame_instance_element_repeat where frame_instance_id = " + frameInstanceID + " and "
			+ "element_id = " + elementID);
				
		int repeatNum = -1;
		if (rs.next()) {
			repeatNum = rs.getInt(1);
			//repeatNum += increment;
		}
				
		if (repeatNum == -1) {
			stmt.execute("insert into " + schema + "frame_instance_element_repeat (frame_instance_id, element_id, section_slot_num, repeat_num) values ('" + frameInstanceID + "',"
				+ elementID + "," + sectionSlotNum + ",1)");
		}
		
		else {
			repeatNum += increment;
			if (repeatNum >= 0)
				stmt.execute("update " + schema + "frame_instance_element_repeat set repeat_num = " + repeatNum + " where frame_instance_id = " + frameInstanceID + " and "
					+ "element_id = " + elementID);
		}
		
		stmt.close();
		conn.close();
	
		return readCRFDB(crfID, sectionList, frameInstanceID);
	}
	
	private void getConnection()
	{
		//conn = DB.getConnection();
	}
	
	private List<Map<String, Object>> getElementDefs(List<Map<String, Object>> elementList, String sectionName) throws SQLException
	{
		System.out.println("Section: " + sectionName);
		List<Map<String, Object>> htmlElements = new ArrayList<Map<String, Object>>();
		for (int i=0; i<elementList.size(); i++) {
			Map<String, Object> elementMap = elementList.get(i);
			System.out.println("Element: " + elementMap.get("name"));
			htmlElements.add(getElementDef(elementMap, sectionName));
		}
		
		return htmlElements;
	}
	
	private Map<String, Object> getElementDef(Map<String, Object> elementMap, String sectionName) throws SQLException
	{
		Map<String, Object> element = new HashMap<String, Object>();
		element.put("section", sectionName);
		element.put("display", (String) elementMap.get("display"));
		String name = (String) elementMap.get("name");
		String htmlID = generateID(name);
		element.put("htmlID", htmlID);
		element.put("dataType", elementMap.get("dataType"));
		element.put("display", elementMap.get("display"));
		
		String type = (String) elementMap.get("type");
		int elementTypeID = getElementType(type);
		element.put("elementType", elementTypeID);
		String dataType = (String) elementMap.get("dataType");
		int dataTypeID = getDataType(dataType);
		element.put("dataType", dataTypeID);
		
		element.put("annotation", elementMap.get("annotation"));
		element.put("primaryKey", elementMap.get("primaryKey"));
		
		List<Object> values = (List<Object>) elementMap.get("values");
		if (values != null)
			element.put("values", getValues(htmlID, values));
		
		Object repeatObj = elementMap.get("repeat");
		if (repeatObj != null)
			element.put("repeat", ((Double) repeatObj).intValue());
		
		/*
		if (type.equals("text")) {
			//element.put("html", "<input type=''text'' id=''" + htmlID + "'' />");
			element.put("elementType", "text");
		}
		else if (type.equals("radio")) {
			//element.put("values", getValues(htmlID, values));
			element.put("elementType", "radio");
		}
		else if (type.equals("checkbox")) {
			//element.put("values", getValues(htmlID, values));
			element.put("elementType", "checkbox");
		}
		else if (type.equals("date")) {
			//element.put("html", "<input type=''date'' id=''" + htmlID + "'' />");
			element.put("elementType", "date");
		}
		*/		
		
		return element;
	}
	
	private void insertSlots(int elementID, List<Object> values)
	{
		
	}
	
	private List<Map<String, String>> getValues(String htmlID, List<Object> values)
	{
		//Map<String, Object> radioMap = new HashMap<String, Object>();
		//StringBuilder strBlder = new StringBuilder();
		//List<Object> values = (List<Object>) map.get("values");
		
		List<Map<String, String>> valueMaps = new ArrayList<Map<String, String>>(); 
		for (Object value : values) {
			Map<String, String> valueMap = new HashMap<String, String>();

			String valueStr = "";
			String condition = "";
			if (value instanceof String) {
				valueStr = (String) value;
			}
			
			else if (value instanceof Map) {
				Map<String, Object> vMap = (Map<String, Object>) value;
				valueStr = (String) vMap.get("display");
				valueMap.put("condition", (String) vMap.get("condition"));
			}
			
			//String valueStr = getValueString(value);
			String id = generateID(htmlID + "_" + valueStr);
			//strBlder.append("<input type='radio' name='" + htmlID + "' id='" + id + "' value='" + valueStr + "'>" + valueStr + "</input><br>\n");
			valueMap.put("display", valueStr);
			valueMap.put("htmlID", id);
			valueMaps.add(valueMap);
		}
		
		//radioMap.put("values", valueMaps);
		//radioMap.put("html", strBlder.toString());
		
		return valueMaps;
	}
	
	private String getValueString(Object value)
	{
		String valueStr = "";
		
		if (value instanceof String)
			valueStr = (String) value;
		
		else if (value instanceof Map) {
			Map<String, Object> valueMap = (Map<String, Object>) value;
			valueStr = (String) valueMap.get("display");
		}
		
		
		return valueStr;
	}
	
	private String getHTMLString(String elementType, String htmlID, List<Map<String, String>> values, int repeat, int elementRepeatNum, String repeatStr)
	{
		System.out.println("htmlID: " + htmlID + " repeat: " + repeat + " elementRepeatNum: " + elementRepeatNum);
		String htmlStr = null;
		if (elementType.equals("radio") || elementType.equals("checkbox")) {
			htmlStr = "<form id='" + htmlID + "'>" + getMultiValueHTML(elementType, htmlID, values, repeatStr) + " </form>";
			if (repeat == -1 || repeat > 1)
				htmlStr += "<input type='button' id='" + htmlID + "_add' value='+' onclick='addElement(this.id)'/>";
		}
		else if (elementType.equals("text")) {
			htmlStr = "<input type='text' id='" + htmlID + "' " + " name='" + htmlID + "' />";
			if ((repeat == -1 || repeat > 1) && elementRepeatNum ==  0)
				htmlStr += "<input type='button' id='" + htmlID + "_add' value='+' onclick='addElement(this.id)'/>";
			if (elementRepeatNum > 0)
				htmlStr += "<input type='button' id='" + htmlID + "_remove' value='-' onclick='removeElement(this.id)'/>";
		}
		else if (elementType.equals("date")) {
			htmlStr = "<input type='date' id='" + htmlID + "' name='" + htmlID + "' />";
			if ((repeat == -1 || repeat > 1) && elementRepeatNum ==  0)
				htmlStr += "<input type='button' id='" + htmlID + "_add' value='+' onclick='addElement(this.id)'/>";
			if (elementRepeatNum > 0)
				htmlStr += "<input type='button' id='" + htmlID + "_remove' value='-' onclick='removeElement(this.id)'/>";
		}
		else if (elementType.equals("textarea")) {
			htmlStr = "<textarea id='" + htmlID + "' name='" + htmlID + "'></textarea>";
			if ((repeat == -1 || repeat > 1) && elementRepeatNum ==  0)
				htmlStr += "<input type='button' id='" + htmlID + "_add' value='+' onclick='addElement(this.id)'/>";
			if (elementRepeatNum > 0)
				htmlStr += "<input type='button' id='" + htmlID + "_remove' value='-' onclick='removeElement(this.id)'/>";
		}
		
		return htmlStr;
	}
	
	private String getMultiValueHTML(String elementType, String htmlID, List<Map<String, String>> values, String repeatStr)
	{
		StringBuilder strBlder = new StringBuilder();
		for (Map<String, String> value : values) {
			String display = value.get("display");
			String valueHTMLID = value.get("htmlID");
			
			//if (repeatNum > 0)
			valueHTMLID = valueHTMLID + "_"	+ repeatStr;
			
			strBlder.append("<input type='" + elementType + "' name='" + htmlID + "' id='" + valueHTMLID + "' value='" + display + "' onclick='valueClick(event)' "
				+ "onmouseover='valueMouseover(this)'>" + display + "</input><br>\n");
		}
		
		return strBlder.toString();
	}
	
	private String generateID(String name)
	{
		StringBuilder strBlder = new StringBuilder(name.toLowerCase());
		
		return strBlder.toString().replaceAll("( )+", "_");
	}
	
	private int writeCRFToDB(String crfName, List<Map<String, Object>> sectionList, List<Map<String, Object>> dataList) throws SQLException
	{
		rq = getReservedQuote();
		int crfID = insertCRF(crfName, generateID(crfName));
		PreparedStatement pstmt = conn.prepareStatement("insert into " + schema + "crf_section (name, crf_id, " + rq + "repeat" + rq + ") values (?,?,?)");
		pstmt.setInt(2, crfID);

		for (Map<String, Object> sectionMap : sectionList) {
			
			pstmt.setString(1, (String) sectionMap.get("name"));
			Integer repeat = (Integer) sectionMap.get("repeat");
			if (repeat == null)
				repeat = 0;
			
			pstmt.setInt(3, repeat);
			
			pstmt.execute();
		}

		pstmt = conn.prepareStatement("insert into " + schema + "crf_element (crf_id, element_id) values (?,?)");
		pstmt.setInt(1, crfID);
		for (Map<String, Object> element : dataList) {
			String section = (String) element.get("section");
			int sectionID = getSectionID(section);
			int elementID = insertElement(crfID, sectionID, element);
			pstmt.setInt(2, elementID);
			pstmt.execute();
		}
		
		return crfID;
	}
	
	private int getSectionID(String section) throws SQLException
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select section_id from " + schema + "crf_section where name = '" + section + "'");
		int sectionID = -1;
		if (rs.next()) {
			sectionID = rs.getInt(1);
		}
		
		/*
		if (sectionID == -1) {
			stmt.execute("insert into crf_section (name) values ('" + section + "')");
			rs = stmt.executeQuery("select last_insert_id()");
			if (rs.next()) {
				sectionID = rs.getInt(1);
			}
		}
		*/
		
		stmt.close();
		
		return sectionID;
	}
	
	private int insertSection(int crfID, String sectionName, int repeat) throws SQLException
	{
		int sectionID = -1;
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select section_id from " + schema + "crf_section where name = '" + sectionName + "'");
		if (rs.next()) {
			sectionID = rs.getInt(1);
		}
		
		if (sectionID == -1) {
			stmt.execute("insert into " + schema + "crf_section (name, crf_id, " + rq + "repeat" + rq + ") values ('" + sectionName + "'," + crfID + "," + repeat + ")");
		
			sectionID = getLastID();
			/*
			rs = stmt.executeQuery("select last_insert_id()");
			if (rs.next())
				sectionID = rs.getInt(1);
				*/
		}
		
		stmt.close();
		
		return sectionID;
	}
	
	private int getElementType(String elementTypeName) throws SQLException
	{
		int elementType = -1;
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select element_type_id from " + schema + "element_type where element_type_name = '" + elementTypeName + "'");
		if (rs.next()) {
			elementType = rs.getInt(1);
		}
		
		return elementType;
	}
	
	private int getDataType(String dataTypeName) throws SQLException
	{
		int dataType = -1;
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select id from " + schema + "data_type where name = '" + dataTypeName + "'");
		if (rs.next()) {
			dataType = rs.getInt(1);
		}
		
		return dataType;
	}
	
	private int insertCRF(String crfName, String htmlID) throws SQLException
	{
		int crfID = -1;
		Statement stmt = conn.createStatement();
		stmt.execute("insert into " + schema + "crf (name, html_id) values ('" + crfName + "', '" + htmlID + "')");
		
		crfID = getLastID();
		
		/*
		ResultSet rs = stmt.executeQuery("select last_insert_id()");
		if (rs.next()) {
			crfID = rs.getInt(1);
		}
		*/
		
		stmt.close();
		
		return crfID;
	}
	
	private int insertElement(int crfID, int sectionID, Map<String, Object> element) throws SQLException
	{
		String htmlID = (String) element.get("htmlID");
		String displayName = (String) element.get("display");
		Integer repeat = (Integer) element.get("repeat");
		int elementType = (Integer) element.get("elementType");
		int dataType = (Integer) element.get("dataType");
		Boolean primaryKey = (Boolean) element.get("primaryKey");
		
		if (repeat == null)
			repeat = 1;
		
		if (primaryKey == null)
			primaryKey = false;
		
		int primaryInt = 0;
		if (primaryKey)
			primaryInt = 1;

		
		String annot = (String) element.get("annotation");
		
		Statement stmt = conn.createStatement();
		stmt.execute("insert into " + schema + "element (display_name, html_id, section_id, " + rq + "repeat" + rq + ", element_type, data_type, primary_key) "
			+ "values ('" + displayName + "','" + htmlID + "'," + sectionID + "," + repeat + "," + elementType + "," + dataType + "," + primaryInt + ")");
		
		int elementID = getLastID();
		
		/*
		ResultSet rs = stmt.executeQuery("select last_insert_id()");
		if (rs.next()) {
			elementID = rs.getInt(1);
		}
		*/
		
		slotAnnotMap.put(elementID, annot);
		
		PreparedStatement pstmt = conn.prepareStatement("insert into " + schema + "element_value (element_id, value_id) values (?,?)");
		pstmt.setInt(1, elementID);
		
		List<Map<String, String>> values = (List<Map<String, String>>) element.get("values");
		if (values != null) {
			for (Map<String, String> value : values) {
				int valueID = insertValue(value);
				pstmt.setInt(2, valueID);
				pstmt.execute();
			}
		}
		else {
			Map<String, String> value = new HashMap<String, String>();
			value.put("display", displayName);
			value.put("htmlID", htmlID);
			int valueID = insertValue(value);
			pstmt.setInt(2, valueID);
			pstmt.execute();
		}
	
		stmt.close();
		pstmt.close();
		
		return elementID;
	}
	
	private int insertValue(Map<String, String> value) throws SQLException
	{
		Statement stmt = conn.createStatement();
		String displayName = (String) value.get("display");
		String htmlID = (String) value.get("htmlID");
		String cond = (String) value.get("condition");
		if (cond == null)
			cond = "";
		

		stmt.execute("insert into " + schema + "value (display_name, html_id) values ('" + displayName + "', '" + htmlID + "')");
		
		int valueID = getLastID();
		
		/*
		ResultSet rs = stmt.executeQuery("select last_insert_id()");
		if (rs.next())
			valueID = rs.getInt(1);
			*/
		
		slotCondMap.put(valueID, cond);
		
		stmt.close();
		
		return valueID;
	}
	
	private List<Map<String, Object>> readCRFFromDB(int crfID, List<Map<String, Object>> sectionList, int frameInstanceID) throws SQLException
	{
		dataList = new ArrayList<Map<String, Object>>();
		rq = getReservedQuote();
		
		
		/*
		int crfID = -1;
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select crf_id from crf where name = '" + crfName + "'");
		if (rs.next())
			crfID = rs.getInt(1);
			*/
		
		Statement stmt = conn.createStatement();
		
		if (sectionList.size() == 0) {
			//sectionList = new ArrayList<Map<String, Object>>();
			ResultSet rs = stmt.executeQuery("select section_id, name, " + rq + "repeat" + rq + " from " + schema + "crf_section where crf_id = " + crfID + " order by section_id");
			while (rs.next()) {
				Map<String, Object> sectionMap = new HashMap<String, Object>();
				sectionMap.put("sectionID", rs.getInt(1));
				sectionMap.put("name", rs.getString(2));
				sectionMap.put("repeat", rs.getInt(3));
				sectionMap.put("repeatNumber", 0);
				sectionList.add(sectionMap);
			}
		}
		
		char sectionIndex = 65;
		for (Map<String, Object> sectionMap : sectionList) {
			
			int sectionID = ((Number) sectionMap.get("sectionID")).intValue();
			String sectionName = (String) sectionMap.get("name");
			int repeat = ((Number) sectionMap.get("repeat")).intValue();
			int repeatNum = ((Number) sectionMap.get("repeatNumber")).intValue();
			
			System.out.println("sectionID: " + sectionID + " sectionName: " + sectionName + " repeatNumber: " + repeatNum);
		
			ResultSet rs = stmt.executeQuery("select a.element_id, a.display_name, a.html_id, c.element_type_name, a.repeat "
				+ "from " + schema + "element a, " + schema + "crf_section b, " + schema + "element_type c "
				+ "where a.section_id = b.section_id and b.crf_id = " + crfID + " and "
				+ "b.section_id = " + sectionMap.get("sectionID") + " and "
				+ "c.element_type_id = a.element_type "
				+ "order by element_id");
			
			List<Map<String, Object>> sectionDataList = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> element = new HashMap<String, Object>();
				int elementID = rs.getInt(1);
				String htmlID = rs.getString(3);
				
				element.put("elementID", elementID);
				element.put("htmlID", htmlID);
				
				element.put("display", rs.getString(2));
				
				String elementType = rs.getString(4);
				element.put("elementType", elementType);
				element.put("repeat", rs.getInt(5));

				//element.put("repeatNum", getElementRepeatNumber(frameInstanceID, elementID));
				
				sectionDataList.add(element);
			}
			
			for (int i=0; i<repeatNum+1; i++) {
				String sectionNameIndex = Character.toString(sectionIndex) + "|" + sectionName;
				System.out.println("section name: " + sectionNameIndex);
				
				for (Map<String, Object> element : sectionDataList) {
					element.put("section", "{\"sectionID\":\"" + sectionIndex + "\",\"sectionName\":\"" + sectionNameIndex + "\",\"repeat\":" + repeat + ",\"repeatIndex\":" + i + "}");

					
					/*
					Map<String, Object> dataListElement = new HashMap<String, Object>();
					dataListElement.put("section", element.get("section"));
					dataListElement.put("display", element.get("display"));
					*/
					
					String elementType = (String) element.get("elementType");
					int elementID = (Integer) element.get("elementID");
					int elementRepeat = (Integer) element.get("repeat");					
					//int elementRepeatNum = (Integer) element.get("repeatNum");
					
					//get element repeat number
					int elementRepeatNum = getElementRepeatNumber(frameInstanceID, elementID, i);
					
					
					//element level repeat
					for (int j=0; j<elementRepeatNum+1; j++) {
						
						Map<String, Object> dataListElement = new HashMap<String, Object>();
						dataListElement.put("section", element.get("section"));
						dataListElement.put("display", element.get("display"));
						
						//String elementType = (String) element.get("elementType");
						//int elementID = (Integer) element.get("elementID");
						
						//int elementRepeat = (Integer) element.get("repeat");
						//int elementRepeatNum = (Integer) element.get("repeatNum");
						
						String repeatStr = i + "_" + j;
					
						String elementIDStr = Integer.toString(elementID) + "_" + repeatStr;
						String htmlID = (String) element.get("htmlID") + "_" + repeatStr;
						
						List<Map<String, String>> values = readValues(elementID);
						dataListElement.put("html", getHTMLString(elementType, htmlID, values, elementRepeat, j, repeatStr));
						
						dataListElement.put("elementID", elementIDStr);
						dataListElement.put("htmlID", htmlID);
						dataListElement.put("elementType", elementType);
						
						dataList.add(dataListElement);
					}
				}
				
				sectionIndex++;
			}
			
		}
		
		stmt.close();
		
		return dataList;
	}
	
	private List<Map<String, String>> readValues(int elementID) throws SQLException
	{
		List<Map<String, String>> values = new ArrayList<Map<String, String>>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select a.display_name, a.html_id "
			+ "from " + schema + "value a, " + schema + "element_value b where a.value_id = b.value_id and b.element_id = " + elementID);
		while (rs.next()) {
			Map<String, String> valueMap = new HashMap<String, String>();
			valueMap.put("display", rs.getString(1));
			valueMap.put("htmlID", rs.getString(2));
			values.add(valueMap);
		}
		
		return values;
	}
	
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
	
	private String getReservedQuote() throws SQLException
	{
		String rq = "`";
		String dbType = conn.getMetaData().getDatabaseProductName();
		if (dbType.equals("Microsoft SQL Server")) {
			rq = "\"";
		}
		
		return rq;
	}
	
	private int getElementRepeatNumber(int frameInstanceID, int elementID, int sectionSlotNum) throws SQLException
	{
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select repeat_num from " + schema + "frame_instance_element_repeat where frame_instance_id = " + frameInstanceID
				+ " and element_id = " + elementID + " and section_slot_num = " + sectionSlotNum);
		
		int elementRepeatNum = 0;
		if (rs.next())
			elementRepeatNum = rs.getInt(1);
		
		stmt.close();
		
		return elementRepeatNum;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 7) {
			System.out.println("usage: user password host dbName dbType schema fileName");
			System.exit(0);
		}
		
		try {
			if (args[5].equals("NULL"))
				args[5]	= "";
			CRFProcessor proc = new CRFProcessor(args[5] + ".");
			proc.readCRFFile(args[0], args[1], args[2], args[3], args[4], args[6]);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
