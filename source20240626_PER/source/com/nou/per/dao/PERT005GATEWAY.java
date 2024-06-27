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
 * Author    : ���      2007/07/31
 * Modification Log :
 * Vers     Date           By             Notes
 *--------- -------------- -------------- ----------------------------------------
 * V0.0.1   2007/07/31     ���           �إߵ{��
 *                                        �s�W getPert005ForUse(Hashtable ht)
 * V0.0.2   2007/10/19     sorge          �s�W getPer108rPrint(Vector vt, Hashtable ht)
 * V0.0.2   2008/02/14     sorge          �s�W getPer015mQueryV2(Vector vt, Hashtable ht)
 * V0.0.3   2008/02/27     lin            �s�W getPer006mQuery(Hashtable ht)
 *--------------------------------------------------------------------------------
 */
public class PERT005GATEWAY {

    /** ��ƱƧǤ覡 */
    private String orderBy = "";
    private DBManager dbmanager = null;
    private Connection conn = null;
    /* ���� */
    private int pageNo = 0;
    /** �C������ */
    private int pageSize = 0;

    /** �O���O�_���� */
    private boolean pageQuery = false;

    /** �ΨӦs�� SQL �y�k������ */
    private StringBuffer sql = new StringBuffer();

    /** <pre>
     *  �]�w��ƱƧǤ覡.
     *  Ex: "AYEAR, SMS DESC"
     *      ���H AYEAR �ƧǦA�H SMS �˧ǧǱƧ�
     *  </pre>
     */
    public void setOrderBy(String orderBy) {
        if(orderBy == null) {
            orderBy = "";
        }
        this.orderBy = orderBy;
    }

    /** ���o�`���� */
    public int getTotalRowCount() {
        return Page.getTotalRowCount();
    }

    /** �����\�إߪŪ����� */
    private PERT005GATEWAY() {}

    /** �غc�l�A�d�ߥ�����ƥ� */
    public PERT005GATEWAY(DBManager dbmanager, Connection conn) {
        this.dbmanager = dbmanager;
        this.conn = conn;
    }

    /** �غc�l�A�d�ߤ�����ƥ� */
    public PERT005GATEWAY(DBManager dbmanager, Connection conn, int pageNo, int pageSize) {
        this.dbmanager = dbmanager;
        this.conn = conn;
        this.pageNo = pageNo;
        this.pageSize = pageSize;
        pageQuery = true;
    }

    /**
     *
     * @param ht �����
     * @return �^�� Vector ����A���e�� Hashtable �����X�A<br>
     *         �C�@�� Hashtable �� KEY �����W�١AKEY ���Ȭ���쪺��<br>
     *         �Y����즳����W�١A�h�� KEY �Х[�W _NAME, EX: SMS �䤤�����г]�� SMS_NAME
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
                // �̤������X���
                rs = Page.getPageResultSet(dbmanager, conn, sql.toString(), pageNo, pageSize);
            } else {
                // ���X�Ҧ����
                rs = dbmanager.getSimpleResultSet(conn);
                rs.open();
                rs.executeQuery(sql.toString());
            }
            Hashtable rowHt = null;
            while (rs.next()) {
                rowHt = new Hashtable();
                /** �N���ۤ@���L�h */
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
				"AND   (A.RMK IS NULL OR A.RMK <> '����') "
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
				"decode(Z9.CODE_NAME,'�y��R','�s��','�v��TV','�v��(�q��)',Z9.CODE_NAME) AS CPRODUCE_TYPE,G.CRS_STATUS,e.RMK " +
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
				"AND   (A.RMK IS NULL OR A.RMK <> '����') "
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
        
        
    /** per006m �d�� */
    public Vector getPer006mQuery(Hashtable ht) throws Exception
    {
        DBResult    rs  =   null;
        Vector      vt  =   new Vector();
        
        try
        {
            //����
            String  AYEAR           =   Utility.checkNull(ht.get("AYEAR"), "");             //�Ǧ~
            String  SMS             =   Utility.checkNull(ht.get("SMS"), "");               //�Ǵ�
            String  PRODUCE_TYPE    =   Utility.checkNull(ht.get("PRODUCE_TYPE"), "");      //�s�@���A
            String  CRSNO           =   Utility.checkNull(ht.get("CRSNO"), "");             //��إN��
            
            
            if (sql.length() > 0)
                sql.delete(0, sql.length());
            
            sql.append(
            	// ��Ǧ~���bpert005���i�d���ƪ���ؤ@�w�O�s�}
            	"select a.AYEAR,a.sms,a.CRSNO, NVL(b.CRS_NAME,MEDIA_AGENDANAME) AS CRS_NAME ,A.MEDIA_AGENDASEQ,A.MEDIA_AGENDANAME,E.NAME "+
	            "from pert005 a, cout002 b, TRAT001 E "+
	            "where   b.CRSNO(+)=a.CRSNO  "+
	                "and a.AYEAR='"+AYEAR+"'  "+
	                "and a.SMS='"+SMS+"'  "+
	                "and a.PRODUCE_TYPE like '%"+PRODUCE_TYPE+"' "+
	                "and a.CRSNO like '%"+CRSNO+"' "+
	                "AND E.IDNO = A.BID  "+
	            "union  "+
	            // ���o��}���---�`����Ƥ@�˱qPERT005���o(�u�O���o�Ӭ�س̪�Ǧ~���Ҷ}�]������)
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