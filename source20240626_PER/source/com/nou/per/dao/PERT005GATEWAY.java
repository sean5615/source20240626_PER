package com.nou.per.dao;

import com.acer.db.DBManager;
import com.acer.db.query.DBResult;
import com.acer.util.Utility;
import com.acer.apps.Page;

import java.sql.Connection;
import java.util.Vector;
import java.util.Hashtable;

/*
 * (PERT005) Gateway/*
 *-------------------------------------------------------------------------------*
 * Author    : 國長      2007/07/31
 * Modification Log :
 * Vers     Date           By             Notes
 *--------- -------------- -------------- ----------------------------------------
 * V0.0.1   2007/07/31     國長           建立程式
 *                                        新增 getPert005ForUse(Hashtable ht)
 * V0.0.2   2007/10/19     sorge          新增 getPer108rPrint(Vector vt, Hashtable ht)
 * V0.0.2   2008/02/14     sorge          新增 getPer015mQueryV2(Vector vt, Hashtable ht)
 * V0.0.3   2008/02/27     lin            新增 getPer006mQuery(Hashtable ht)
 *--------------------------------------------------------------------------------
 */
public class PERT005GATEWAY {

    /** 資料排序方式 */
    private String orderBy = "";
    private DBManager dbmanager = null;
    private Connection conn = null;
    /* 頁數 */
    private int pageNo = 0;
    /** 每頁筆數 */
    private int pageSize = 0;

    /** 記錄是否分頁 */
    private boolean pageQuery = false;

    /** 用來存放 SQL 語法的物件 */
    private StringBuffer sql = new StringBuffer();

    /** <pre>
     *  設定資料排序方式.
     *  Ex: "AYEAR, SMS DESC"
     *      先以 AYEAR 排序再以 SMS 倒序序排序
     *  </pre>
     */
    public void setOrderBy(String orderBy) {
        if(orderBy == null) {
            orderBy = "";
        }
        this.orderBy = orderBy;
    }

    /** 取得總筆數 */
    public int getTotalRowCount() {
        return Page.getTotalRowCount();
    }

    /** 不允許建立空的物件 */
    private PERT005GATEWAY() {}

    /** 建構子，查詢全部資料用 */
    public PERT005GATEWAY(DBManager dbmanager, Connection conn) {
        this.dbmanager = dbmanager;
        this.conn = conn;
    }

    /** 建構子，查詢分頁資料用 */
    public PERT005GATEWAY(DBManager dbmanager, Connection conn, int pageNo, int pageSize) {
        this.dbmanager = dbmanager;
        this.conn = conn;
        this.pageNo = pageNo;
        this.pageSize = pageSize;
        pageQuery = true;
    }

    /**
     *
     * @param ht 條件值
     * @return 回傳 Vector 物件，內容為 Hashtable 的集合，<br>
     *         每一個 Hashtable 其 KEY 為欄位名稱，KEY 的值為欄位的值<br>
     *         若該欄位有中文名稱，則其 KEY 請加上 _NAME, EX: SMS 其中文欄位請設為 SMS_NAME
     * @throws Exception
     */
    public Vector getPert005ForUse(Hashtable ht) throws Exception {
        if(ht == null) {
            ht = new Hashtable();
        }
        Vector result = new Vector();
        if(sql.length() > 0) {
            sql.delete(0, sql.length());
        }
        sql.append(
            "SELECT P05.MEDIA_CODE, P05.MEDIA_AGENDASEQ, P05.MEDIA_AGENDANAME, P05.BID, P05.BNAME " +
            "FROM PERT005 P05 " +
            "WHERE 1 = 1 "
        );
        if(!Utility.nullToSpace(ht.get("MEDIA_CODE")).equals("")) {
            sql.append("AND P05.MEDIA_CODE = '" + Utility.nullToSpace(ht.get("MEDIA_CODE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("MEDIA_AGENDASEQ")).equals("")) {
            sql.append("AND P05.MEDIA_AGENDASEQ = '" + Utility.nullToSpace(ht.get("MEDIA_AGENDASEQ")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("MEDIA_AGENDANAME")).equals("")) {
            sql.append("AND P05.MEDIA_AGENDANAME = '" + Utility.nullToSpace(ht.get("MEDIA_AGENDANAME")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("BID")).equals("")) {
            sql.append("AND P05.BID = '" + Utility.nullToSpace(ht.get("BID")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("BNAME")).equals("")) {
            sql.append("AND P05.BNAME = '" + Utility.nullToSpace(ht.get("BNAME")) + "' ");
        }

        if(!orderBy.equals("")) {
            String[] orderByArray = orderBy.split(",");
            for(int i = 0; i < orderByArray.length; i++) {
                orderByArray[i] = "P05." + orderByArray[i].trim();

                if(i == 0) {
                    orderBy += "ORDER BY ";
                } else {
                    orderBy += ", ";
                }
                orderBy += orderByArray[i].trim();
            }
            sql.append(orderBy.toUpperCase());
            orderBy = "";
        }

        DBResult rs = null;
        try {
            if(pageQuery) {
                // 依分頁取出資料
                rs = Page.getPageResultSet(dbmanager, conn, sql.toString(), pageNo, pageSize);
            } else {
                // 取出所有資料
                rs = dbmanager.getSimpleResultSet(conn);
                rs.open();
                rs.executeQuery(sql.toString());
            }
            Hashtable rowHt = null;
            while (rs.next()) {
                rowHt = new Hashtable();
                /** 將欄位抄一份過去 */
                for (int i = 1; i <= rs.getColumnCount(); i++)
                    rowHt.put(rs.getColumnName(i), rs.getString(i));

                result.add(rowHt);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
        return result;
    }

	public void getPer108rPrint(Vector vt, Hashtable ht) throws Exception {
		DBResult rs = null;
		try
		{
			if(sql.length() >0)
				sql.delete(0, sql.length());

			sql.append
			(
				"SELECT DISTINCT " +
				"d.FACULTY_CODE,a.CRSNO, d.CRS_NAME, e.WEEK_ITEM, substr(e.DDMM,5,2) AS MM, substr(e.DDMM,7,2) AS DD, a.WEEK, " +
				"e.MEDIA_AGENDASEQ, f.MEDIA_AGENDANAME,H.NEW_REWORK, " +
				"f.BID, g.NAME, e.RMK " +
				"FROM PERT003 a " +
				"JOIN COUT002 d ON a.CRSNO=d.CRSNO " +
				"JOIN COUT001 h ON a.AYEAR=h.AYEAR AND a.SMS=h.SMS AND a.CRSNO=h.CRSNO " +
				"JOIN PERT007 e ON a.AYEAR=e.AYEAR AND a.SMS=e.SMS AND a.CRSNO=e.CRSNO AND a.PRODUCE_TYPE=e.PRODUCE_TYPE " +
				"JOIN PERT005 f ON e.PERT005_AYEAR=f.AYEAR AND e.PERT005_SMS=f.SMS AND e.CRSNO=f.CRSNO AND e.MEDIA_AGENDASEQ=f.MEDIA_AGENDASEQ " +
				"JOIN TRAT001 g ON f.BID=g.IDNO " +
				"WHERE 1=1 "+
				"AND   (A.RMK IS NULL OR A.RMK <> '重播') "
			);

			sql.append("AND a.AYEAR = '" + Utility.dbStr(ht.get("AYEAR")) + "' ");
			sql.append("AND a.SMS = '" + Utility.dbStr(ht.get("SMS")) + "' ");

			if(!Utility.checkNull(ht.get("PRODUCE_TYPE"), "").equals(""))
				sql.append("AND a.PRODUCE_TYPE = '" + Utility.dbStr(ht.get("PRODUCE_TYPE")) + "' ");	
			if(!Utility.checkNull(ht.get("CRSNO"), "").equals(""))
				sql.append("AND a.CRSNO = '" + Utility.dbStr(ht.get("CRSNO")) + "' ");	
			
			sql.append("ORDER BY DECODE(d.FACULTY_CODE, '80', '01', '90', '02', d.FACULTY_CODE), " );
			sql.append("DECODE(h.NEW_REWORK,'1','1','6','2','3','3','4','3','5','5','2','6'),a.CRSNO, e.MEDIA_AGENDASEQ " );

			if(pageQuery) {
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(), pageNo, pageSize);
			} else {
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}

			Hashtable rowHt = null;
			while (rs.next())
			{
				rowHt = new Hashtable();
				for (int i = 1; i <= rs.getColumnCount(); i++)
					rowHt.put(rs.getColumnName(i), rs.getString(i));
				vt.add(rowHt);
			}

		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			if (rs != null)
				rs.close();
		}
	}
	
	public void getPer108rExport(Vector vt, Hashtable ht) throws Exception {
		DBResult rs = null;
		try
		{
			if(sql.length() >0)
				sql.delete(0, sql.length());

			sql.append
			(
				"SELECT DISTINCT " +
				"A.AYEAR,Z6.CODE_NAME AS CSMS,d.FACULTY_CODE,Z2.FACULTY_NAME,a.CRSNO,d.CRS_NAME,G.NEW_REWORK,H.CODE_NAME AS NEW_REWORK_NANE,G.CRS_TIMES, " +
				"decode(Z9.CODE_NAME,'語音R','廣播','影音TV','影音(電視)',Z9.CODE_NAME) AS CPRODUCE_TYPE,G.CRS_STATUS,e.RMK " +
				"FROM PERT003 a " +
				"JOIN COUT002 d ON a.CRSNO=d.CRSNO " +
				"JOIN PERT007 e ON a.AYEAR=e.AYEAR AND a.SMS=e.SMS AND a.CRSNO=e.CRSNO AND a.PRODUCE_TYPE=e.PRODUCE_TYPE " +
				"JOIN PERT005 f ON e.PERT005_AYEAR=f.AYEAR AND e.PERT005_SMS=f.SMS AND e.CRSNO=f.CRSNO AND e.MEDIA_AGENDASEQ=f.MEDIA_AGENDASEQ " +
				"JOIN COUT001 G ON G.AYEAR = a.AYEAR AND G.SMS = a.SMS AND G.CRSNO = a.CRSNO " +
				"JOIN SYST001 H ON H.KIND = 'NEW_REWORK' AND CODE = G.NEW_REWORK " +
				"JOIN SYST003 Z2 ON Z2.FACULTY_CODE = D.FACULTY_CODE " +
				"LEFT JOIN SYST001 Z6 ON Z6.KIND = 'SMS' AND Z6.CODE = A.SMS " +
				"LEFT JOIN SYST001 Z9 ON Z9.KIND = 'PRODUCE_CHOOSE' AND Z9.CODE = A.PRODUCE_TYPE " +
				"WHERE 1=1 "+
				"AND   (A.RMK IS NULL OR A.RMK <> '重播') "
			);

			sql.append("AND a.AYEAR = '" + Utility.dbStr(ht.get("AYEAR")) + "' ");
			sql.append("AND a.SMS = '" + Utility.dbStr(ht.get("SMS")) + "' ");

			if(!Utility.checkNull(ht.get("PRODUCE_TYPE"), "").equals(""))
				sql.append("AND a.PRODUCE_TYPE = '" + Utility.dbStr(ht.get("PRODUCE_TYPE")) + "' ");	
			if(!Utility.checkNull(ht.get("CRSNO"), "").equals(""))
				sql.append("AND a.CRSNO = '" + Utility.dbStr(ht.get("CRSNO")) + "' ");	
			
			sql.append("ORDER BY DECODE(d.FACULTY_CODE, '80', '01', '90', '02', d.FACULTY_CODE),DECODE(G.NEW_REWORK,'1','1','6','2','3','3','4','3','5','5','2','6'),a.CRSNO " );

			if(pageQuery) {
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(), pageNo, pageSize);
			} else {
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}

			Hashtable rowHt = null;
			while (rs.next())
			{
				rowHt = new Hashtable();
				for (int i = 1; i <= rs.getColumnCount(); i++)
					rowHt.put(rs.getColumnName(i), rs.getString(i));
				vt.add(rowHt);
			}

		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			if (rs != null)
				rs.close();
		}
	}
	
	public void getPer015mQueryV2(Vector vt, Hashtable ht) throws Exception {
		DBResult rs = null;
		try
		{
			if(sql.length() >0)
				sql.delete(0, sql.length());

			sql.append
			(
				"SELECT a.AYEAR, a.SMS, a.CRSNO, a.WEEK_ITEM, a.DDMM, a.WEEK, a.MEDIA_AGENDASEQ,  " +
					"a.RMK, b.MEDIA_AGENDANAME, c.NAME " +
				"FROM PERT007 a " +
					"JOIN PERT005 b ON b.AYEAR=a.PERT005_AYEAR AND b.SMS=a.PERT005_SMS " +
						"AND a.MEDIA_AGENDASEQ=b.MEDIA_AGENDASEQ AND a.CRSNO=b.CRSNO " +
					"JOIN TRAT001 c ON c.IDNO=b.BID " +
				"WHERE 1=1 "
			);

			sql.append("AND a.AYEAR = '" + Utility.dbStr(ht.get("AYEAR")) + "' ");
			sql.append("AND a.SMS = '" + Utility.dbStr(ht.get("SMS")) + "' ");
			sql.append("AND a.PRODUCE_TYPE = '" + Utility.dbStr(ht.get("PRODUCE_TYPE")) + "' ");
			sql.append("AND a.CRSNO = '" + Utility.dbStr(ht.get("CRSNO")) + "' ");
			sql.append("ORDER BY a.media_agendaseq " );

			if(pageQuery) {
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(), pageNo, pageSize);
			} else {
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}

			Hashtable rowHt = null;
			while (rs.next())
			{
				rowHt = new Hashtable();
				for (int i = 1; i <= rs.getColumnCount(); i++)
					rowHt.put(rs.getColumnName(i), rs.getString(i));
				vt.add(rowHt);
			}

		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			if (rs != null)
				rs.close();
		}
	}
        
        
    /** per006m 查詢 */
    public Vector getPer006mQuery(Hashtable ht) throws Exception
    {
        DBResult    rs  =   null;
        Vector      vt  =   new Vector();
        
        try
        {
            //條件
            String  AYEAR           =   Utility.checkNull(ht.get("AYEAR"), "");             //學年
            String  SMS             =   Utility.checkNull(ht.get("SMS"), "");               //學期
            String  PRODUCE_TYPE    =   Utility.checkNull(ht.get("PRODUCE_TYPE"), "");      //製作型態
            String  CRSNO           =   Utility.checkNull(ht.get("CRSNO"), "");             //科目代號
            
            
            if (sql.length() > 0)
                sql.delete(0, sql.length());
            
            sql.append(
            	// 當學年期在pert005中可查到資料的科目一定是新開
            	"select a.AYEAR,a.sms,a.CRSNO, NVL(b.CRS_NAME,MEDIA_AGENDANAME) AS CRS_NAME ,A.MEDIA_AGENDASEQ,A.MEDIA_AGENDANAME,E.NAME "+
	            "from pert005 a, cout002 b, TRAT001 E "+
	            "where   b.CRSNO(+)=a.CRSNO  "+
	                "and a.AYEAR='"+AYEAR+"'  "+
	                "and a.SMS='"+SMS+"'  "+
	                "and a.PRODUCE_TYPE like '%"+PRODUCE_TYPE+"' "+
	                "and a.CRSNO like '%"+CRSNO+"' "+
	                "AND E.IDNO = A.BID  "+
	            "union  "+
	            // 取得續開科目---節次資料一樣從PERT005取得(只是取得該科目最近學年期所開設的講次)
	            "SELECT b.AYEAR,b.sms,C.CRSNO,C.CRS_NAME,B.MEDIA_AGENDASEQ,B.MEDIA_AGENDANAME,D.NAME "+
	            "FROM COUT001 A,PERT005 B,COUT002 C,TRAT001 D "+
	            "WHERE "+
	                "      A.AYEAR = '"+AYEAR+"'  "+
	                "AND   A.SMS = '"+SMS+"'  "+
	                "AND   A.NEW_REWORK in ('3','4','5') "+
	                "AND   A.EST_RESULT_MK = 'Y' "+
	                "AND   A.OPEN1 = 'Y' "+
	                "and   A.CRSNO like 																																																																	'%"+CRSNO+"' "+
	                "AND   B.CRSNO = A.CRSNO "+
	                "AND   B.PRODUCE_TYPE like '%"+PRODUCE_TYPE+"%' "+
	                "AND   B.AYEAR || B.SMS = (SELECT MAX(C.AYEAR || C.SMS ) FROM PERT005 C WHERE C.CRSNO = B.CRSNO) "+ 
	                "AND   C.CRSNO = A.CRSNO  "+
	                "AND   D.IDNO = B.BID "+
	            "order by 1,3 "
           );

            if(pageQuery)
            {
                rs = Page.getPageResultSet(dbmanager, conn, sql.toString(), pageNo, pageSize);
            }
            else
            {
                rs = dbmanager.getSimpleResultSet(conn);
                rs.open();
                rs.executeQuery(sql.toString());
            }
            
            
            Hashtable rowHt = null;
            
            while (rs.next())
            {
                rowHt = new Hashtable();
                for (int i = 1; i <= rs.getColumnCount(); i++)
                    rowHt.put(rs.getColumnName(i), rs.getString(i));
                vt.add(rowHt);
            }
            
            return vt;
        }
        catch(Exception e)
        {
            throw e;
        }
        finally
        {
            if (rs != null)
                rs.close();
        }
    }
}