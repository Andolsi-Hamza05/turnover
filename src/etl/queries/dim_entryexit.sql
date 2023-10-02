-- generate unique entryexit id 
CREATE OR REPLACE FUNCTION entryexit_key(p_nudoss NUMBER, p_datent DATE, p_datsor DATE) RETURN VARCHAR2 IS
    key_value VARCHAR2(100);
BEGIN
    -- Convert dates to the format YYYYMMDD
    key_value := p_nudoss || '_' || TO_CHAR(p_datent, 'YYYYMMDD') || '_' || TO_CHAR(p_datsor, 'YYYYMMDD');
    RETURN key_value;
END entryexit_key;

-- create a procedure that returns the table 
create or replace PROCEDURE dimentryexit(p_result OUT SYS_REFCURSOR) AS
BEGIN
    OPEN p_result FOR
    SELECT 
        entryexit_key(ZYES.NUDOSS, ZYES.DATENT, ZYES.DATSOR) AS Entry_Exit_Key,
        ZYES.NUDOSS AS Emp_Code,
        ZYES.DATENT AS Entry_Date,
        ZYES.DATSOR AS Exit_Date,
        ZYES.CODENT AS Entry_Pattern_Code,
        ZYES.CODSOR AS Exit_Pattern_Code,
        ZYES.CGSTAT AS Reason_Exit_Code,
        table1.LIB AS LIB_Entry_Pattern,
        table2.LIB AS LIB_Exit_Pattern,
        table3.LIB AS LIB_Reason_Exit
    FROM ZYES
    INNER JOIN (
        SELECT DISTINCT LIB, CODENT
        FROM ( 
            SELECT ZY.nudoss AS EMP_COD, ZY.CODENT AS CODENT, d0.cdcode AS COD_AB_D0,
                d0.cdstco AS COD_REP, d1.libabr AS ABR_LIB, d1.liblon AS LIB, d1.cdlang AS COD_LANG
            FROM ZYES ZY
            LEFT JOIN ZD00 D0 ON ZY.CODENT = D0.CDCODE
            LEFT JOIN ZD01 D1 ON D0.NUDOSS = D1.NUDOSS
            WHERE D0.CDSTCO = 'UIR' AND D1.CDLANG = 'F'
        )
    ) table1 ON ZYES.CODENT = table1.CODENT
    INNER JOIN (
        SELECT DISTINCT LIB, CODSOR
        FROM ( 
            SELECT ZY.nudoss AS EMP_COD, ZY.CODSOR AS CODSOR, d0.cdcode AS COD_AB_D0,
                d0.cdstco AS COD_REP, d1.libabr AS ABR_LIB, d1.liblon AS LIB, d1.cdlang AS COD_LANG
            FROM ZYES ZY
            LEFT JOIN ZD00 D0 ON ZY.CODSOR = D0.CDCODE
            LEFT JOIN ZD01 D1 ON D0.NUDOSS = D1.NUDOSS
            WHERE D0.CDSTCO = 'UIS' AND D1.CDLANG = 'F'
        )
    ) table2 ON ZYES.CODSOR = table2.CODSOR
    INNER JOIN (
        SELECT DISTINCT LIB, CGSTAT
        FROM ( 
            SELECT ZY.nudoss AS EMP_COD, ZY.CGSTAT AS CGSTAT, d0.cdcode AS COD_AB_D0,
                d0.cdstco AS COD_REP, d1.libabr AS ABR_LIB, d1.liblon AS LIB, d1.cdlang AS COD_LANG
            FROM ZYES ZY
            LEFT JOIN ZD00 D0 ON ZY.CGSTAT = D0.CDCODE
            LEFT JOIN ZD01 D1 ON D0.NUDOSS = D1.NUDOSS
            WHERE D0.CDSTCO = 'UAJ' AND D1.CDLANG = 'F'
        )
    ) table3 ON ZYES.CGSTAT = table3.CGSTAT;
END dimentryexit;