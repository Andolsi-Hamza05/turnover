CREATE OR REPLACE PROCEDURE dimcontract AS
BEGIN
    SELECT 
        Contract_Key(ZY0M.NUDOSS, ZY0K.DATCON, ZY0M.DATFIN) AS Contract_Key,
        ZY0M.NUDOSS AS Emp_Code, 
        ZY0M.IDCONT AS Contract_Code,
        ZY0M.DATDEB AS Contract_Start_Date,
        ZY0M.DATFIN AS Contract_End_Date,
        ZY0M.RTPDHR AS Paid_Hours,
        ZY0M.QUALIF AS Contract_Qualif_Code,
        ZY0M.TYPCON AS Contract_Type_Code,
        ZY0K.DATCON AS Date_debut_contrat,
        ZY0K.NUDOSS,
        table1.LIB AS QualifDescription
    FROM ZY0M
    LEFT JOIN ZY0K ON ZY0K.NUDOSS = ZY0M.NUDOSS
    INNER JOIN (
        SELECT DISTINCT LIB, QUALIF
        FROM ( 
            SELECT ZY.nudoss AS EMP_COD, ZY.QUALIF AS QUALIF, d0.cdcode AS COD_AB_D0,
               d0.cdstco AS COD_REP, d1.libabr AS ABR_LIB, d1.liblon AS LIB, d1.cdlang AS COD_LANG
            FROM ZY0M ZY
            LEFT JOIN ZD00 D0 ON ZY.QUALIF = D0.CDCODE
            LEFT JOIN ZD01 D1 ON D0.NUDOSS = D1.NUDOSS
            WHERE D0.CDSTCO = 'UIY' AND D1.CDLANG = 'F'
        )
    ) table1 ON ZY0M.QUALIF = table1.QUALIF;
END dimcontract;
