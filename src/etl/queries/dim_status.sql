CREATE OR REPLACE FUNCTION status_key(p_nudoss NUMBER, p_dtef1s DATE, p_datxxx DATE) RETURN VARCHAR2 IS
    key_value VARCHAR2(100);
BEGIN
    key_value := p_nudoss || '_' || TO_CHAR(p_dtef1s, 'YYYYMMDD') || '_' || TO_CHAR(p_datxxx, 'YYYYMMDD');
    RETURN key_value;
END status_key;


CREATE OR REPLACE FUNCTION target_variable(p_datxxx DATE) RETURN NUMBER IS
    v_result NUMBER;
BEGIN
    IF p_datxxx = TO_DATE('31/12/2999', 'DD/MM/YYYY') THEN
        v_result := 0;
    ELSIF p_datxxx < TRUNC(SYSDATE) THEN
        v_result := 1;
    ELSE
        v_result := NULL;
    END IF;
    
    RETURN v_result;
END target_variable;

CREATE OR REPLACE PROCEDURE dimstatus(p_result OUT SYS_REFCURSOR) AS
BEGIN
    OPEN p_result FOR
    SELECT 
        status_key(ZY1S.NUDOSS, ZY1S.DTEF1S, ZY1S.DATXXX) AS Status_Key,
        ZY1S.NUDOSS AS Emp_Code, 
        ZY1S.DTEF1S AS Status_Start_Date, 
        ZY1S.DATXXX AS Status_End_Date, 
        target_variable(ZY1S.DATXXX) AS Turnover,
        ZY1S.STEMPL AS Status_ID
    FROM ZY1S;
END dimstatus;
"""

"""