CREATE OR REPLACE FUNCTION assignment_key(p_nudoss NUMBER, p_dtef00 DATE, p_dten00 DATE) RETURN VARCHAR2 IS
    key_value VARCHAR2(100);
BEGIN
    key_value := p_nudoss || '_' || TO_CHAR(p_dtef00, 'YYYYMMDD') || '_' || TO_CHAR(p_dten00, 'YYYYMMDD');
    RETURN key_value;
END assignment_key;



create or replace PROCEDURE dimassignment(p_result OUT SYS_REFCURSOR) AS
BEGIN
    OPEN p_result FOR
    SELECT
        assignment_key(ZY3B.NUDOSS, ZY3B.DTEF00, ZY3B.DTEN00) AS Assignment_Key,
        ZY3B.NUDOSS AS Emp_Code,
        ZY3B.DTEF00 AS Assignment_Start_Date,
        ZY3B.DTEN00 AS Assignment_End_Date,
        ZY3B.IDOU00 AS Assignment_OrgUnit_Code,
        ZY3B.IDPS00 AS Assignment_Office_Code,
        ZY3B.IDJB00 AS Assignment_Job_Code
    FROM ZY3B
    INNER JOIN (
        SELECT DISTINCT LIB, IDJB00
        FROM ( 
            SELECT ZY.nudoss AS EMP_COD, d0.IDJB00 AS IDJB00,
                d1.LBJBFE AS ABR_LIB, d1.LBJBLG AS LIB, d1.cdlang AS COD_LANG
            FROM ZY3B ZY
            LEFT JOIN ZC00 D0 ON ZY.IDJB00 = D0.IDJB00
            LEFT JOIN ZC01 D1 ON D0.NUDOSS = D1.NUDOSS
            WHERE D1.CDLANG = 'F')) table1
            ON ZY3B.IDJB00 = table1.IDJB00;
END dimassignment; 