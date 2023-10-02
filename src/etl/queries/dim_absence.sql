-- create the function that generate a unique key  
create or replace FUNCTION Absence_Key(
    p_nudoss IN NUMBER,
    p_datdeb IN DATE,
    p_datfin IN DATE,
    p_motabs IN VARCHAR2
) RETURN VARCHAR2 IS
    v_key VARCHAR2(100);
    v_datdeb_str VARCHAR2(20);
    v_datfin_str VARCHAR2(20);
BEGIN
    -- Convert dates to string in a consistent format
    v_datdeb_str := TO_CHAR(p_datdeb, 'YYYY-MM-DD');
    v_datfin_str := TO_CHAR(p_datfin, 'YYYY-MM-DD');

    -- Generate a unique key
    v_key := p_nudoss || '_' || v_datdeb_str || '_' || v_datfin_str || '_' || p_motabs;

    RETURN v_key;
END Absence_Key;


create or replace PROCEDURE dimabsence(p_cursor OUT SYS_REFCURSOR) AS
BEGIN
    OPEN p_cursor FOR
    SELECT
        absence_key(ZYDA.NUDOSS, ZYDA.DATDEB, ZYDA.DATFIN, ZYDA.MOTABS) AS Absence_Key,
        ZYDA.NUDOSS                                                     AS Emp_Code, 
        ZYDA.MOTABS                                                     AS Absence_Code, 
        ZYDA.UNITE2                                                     AS Absence_Hours,
        MIN(ZYDA.NBRJOU)                                                AS Min_Absence_Days, 
        ZYDA.DATDEB                                                     AS Absence_Start_Date,
        ZYDA.DATFIN                                                     AS Absence_End_Date
    FROM ZYDA 
    GROUP BY absence_key(ZYDA.NUDOSS, ZYDA.DATDEB, ZYDA.DATFIN, ZYDA.MOTABS), ZYDA.NUDOSS, ZYDA.MOTABS, ZYDA.UNITE2, ZYDA.DATDEB, ZYDA.DATFIN;
END dimabsence;

