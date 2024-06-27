<%/*
----------------------------------------------------------------------------------
File Name		: per006m
Author			: JASON
Description		: PER006m_匯入講次主題 - 主要頁面
Modification Log	:

Vers		Date       	By            	Notes
--------------	--------------	--------------	----------------------------------
0.0.1		096/05/03	JASON    	Code Generate Create
----------------------------------------------------------------------------------
*/%>
<%@ page contentType="text/html;charset=UTF-8" pageEncoding="MS950"%>
<%@ include file="/utility/header.jsp"%>
<%@ include file="/utility/titleSetup.jsp"%>
<%
	String queryCrsnoWindow = 
		"select distinct a.CRSNO, nvl(b.CRS_NAME,A.MEDIA_AGENDANAME) as CRS_NAME "+ 
		"from pert005 a, cout002 b "+ 
		"where  b.CRSNO(+)= a.CRSNO "+ 			    
			    "and a.AYEAR='[AYEAR]'  "+ 
			    "and a.SMS='[SMS]'  "+ 
			    "and a.PRODUCE_TYPE like '%[PRODUCE_TYPE]' "+ 
		"union  "+ 
		"SELECT distinct C.CRSNO,C.CRS_NAME "+ 
		"FROM COUT001 A,PERT005 B,COUT002 C "+ 
		"WHERE "+ 
		"	          A.AYEAR = '[AYEAR]'  "+ 
		"	    AND   A.SMS = '[SMS]' "+  
		"	    AND   A.NEW_REWORK IN ('3','4','5') "+ 
		"	    AND   A.EST_RESULT_MK = 'Y' "+ 
		"	    AND   A.OPEN1 = 'Y' "+ 
		"	    AND   B.CRSNO = A.CRSNO "+ 
		"	    AND   B.PRODUCE_TYPE like '%[PRODUCE_TYPE]' "+ 
		"	    AND   B.AYEAR || B.SMS = (SELECT MAX(C.AYEAR || C.SMS ) FROM PERT005 C WHERE C.CRSNO = B.CRSNO) "+  
		"	    AND   C.CRSNO = A.CRSNO "+ 
		"	order by 1,2 ";
		
  
		
	session.setAttribute("PER006M_01_SELECT", "PER#SELECT CODE AS SELECT_VALUE, CODE_NAME AS SELECT_TEXT FROM SYST001 WHERE KIND='[KIND]' ORDER BY CODE");
	session.setAttribute("PER006M_02_WINDOW", "PER#"+queryCrsnoWindow);
	session.setAttribute("PER006M_02_BLUR",   "PER#SELECT COUT002.CRS_NAME FROM COUT001 JOIN COUT002 ON COUT001.CRSNO = COUT002.CRSNO WHERE COUT001.AYEAR='[AYEAR]' AND COUT001.SMS='[SMS]' AND CRSNO = '[CRSNO]' AND COUT001.PRODUCE_TYPE like '[PRODUCE_TYPE]' AND COUT001.OPEN_YN='Y' AND COUT001.EST_RESULT_MK='Y'  order by cout001.crsno");
	
	session.setAttribute("PER006M_03_SELECT", "PER#SELECT FACULTY_CODE  AS SELECT_VALUE, FACULTY_NAME AS SELECT_TEXT FROM SYST003  ORDER BY SELECT_VALUE, SELECT_TEXT ");
	session.setAttribute("PER006M_04_WINDOW", "PER#SELECT COUT001.CRSNO, COUT002.CRS_NAME FROM COUT001 JOIN COUT002 ON COUT001.CRSNO = COUT002.CRSNO WHERE COUT001.AYEAR = '[AYEAR]' AND COUT001.SMS = '[SMS]' AND COUT001.FACULTY_CODE = '[FACULTY_CODE]' AND COUT001.OPEN_YN='Y' AND COUT001.EST_RESULT_MK='Y' order by cout001.crsno");

	String	keyParam	=	com.acer.util.Utility.checkNull(request.getParameter("keyParam"), "");

	/**學年學期初始*/
    	java.text.SimpleDateFormat dateTimeFormat = new java.text.SimpleDateFormat("yyyyMMdd");
	java.util.Calendar cal = java.util.Calendar.getInstance();
	String today = dateTimeFormat.format(cal.getTime());
   	com.acer.log.MyLogger logger = new com.acer.log.MyLogger("PER035R");
    	com.acer.db.DBManager dbManager = new com.acer.db.DBManager(logger);  
	com.nou.sys.SYSGETSMSDATA sys = new com.nou.sys.SYSGETSMSDATA(dbManager);
	sys.setSYS_DATE(today);
	// 1.當期 2.前期 3.後期 4.前學年 5.後學年	
	sys.setSMS_TYPE("1");
	int result = sys.execute();

	if(result == 1) 
	{
        		if(!keyParam.equals("") && keyParam.length() > 0) 
        		{
            		keyParam += "&AYEAR=" + sys.getAYEAR() + "&SMS=" + sys.getSMS();
        		} 
        		else 
        		{
            		keyParam = "?AYEAR=" + sys.getAYEAR() + "&SMS=" + sys.getSMS();
        		}
	}		
%>
<script>
	top.viewFrame.location.href	=	'about:blank';
	top.hideView();
	/** 導向第一個處理的頁面 */
	top.mainFrame.location.href	=	'per006m_01v1.jsp<%=keyParam%>';
	/** 導向編輯頁面 */
	top.viewFrame.location.href	=	'per006m_02v1.jsp';
</script>