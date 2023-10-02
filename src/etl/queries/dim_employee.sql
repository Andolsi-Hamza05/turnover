-- UDF CalculateEmpSeniorityDate returning the seniority date of an employee
create or replace FUNCTION CalculateEmpSeniorityDate(
    p_nudoss IN NUMBER,
    p_datan1 IN DATE,
    p_datan4 IN DATE
) RETURN DATE IS
    v_seniority_date DATE;
BEGIN
    IF p_datan4 = TO_DATE('01/01/0001', 'MM/DD/YYYY') THEN
        v_seniority_date := p_datan1;
    ELSE
        v_seniority_date := p_datan4;
    END IF;

    RETURN v_seniority_date;
END CalculateEmpSeniorityDate;


-- A UDF that generate a unique key for each employee
create or replace FUNCTION Employee_key(
    p_nudoss IN NUMBER
) RETURN VARCHAR2 IS
    v_unique_id VARCHAR2(100);
BEGIN
    -- Concatenate the input value to generate the unique ID
    v_unique_id := 'EMP_' || TO_CHAR(p_nudoss);

    RETURN v_unique_id;
END Employee_key;


-- Stored procedure that returns the dimensional table dimemployee  
create or replace PROCEDURE dimemployee(
    p_cursor OUT SYS_REFCURSOR
) AS
BEGIN
    OPEN p_cursor FOR
    SELECT DISTINCT
        employee_key(ZY00.NUDOSS)                                       AS Employee_key,
        ZY00.MATCLE || ZY00.NOMUSE || ZY00.PRENOM                       AS Employee,
        ZY00.NUDOSS                                                     AS Emp_Code,
        ZY00.MATCLE                                                     AS Emp_Matricule,
        ZY00.PRENOM                                                     AS Emp_FirstName,
        ZY00.NOMUSE                                                     AS Emp_LastName,
        ZY00.TYPDOS                                                     AS Emp_Type,
        ZYAU.NUDOSS                                                     AS Emp_Code1,
        ZYAU.MTSAL                                                      AS Emp_Salary,
        ZYAU.mtsard                                                     AS Salary_Currency,
        ZY10.SEXEMP                                                     AS Emp_Gender_Code, 
        ZY10.PAYNAI                                                     AS Emp_CountryOfBirth_Code,
        ZY10.DATNAI                                                     AS Emp_Birth_Date,
        CalculateEmpSeniorityDate(ZY19.NUDOSS, ZY19.DATAN1, ZY19.DATAN4) AS Emp_Seniority_Date,
        ZY19.NUDOSS                                                     AS Emp_Code2  -- Alias for TransformedEmployees table
    FROM ZY00
    LEFT JOIN ZYAU ON ZY00.NUDOSS = ZYAU.NUDOSS
    LEFT JOIN ZY10 ON ZY00.NUDOSS = ZY10.NUDOSS
    LEFT JOIN ZY19 ON ZY00.NUDOSS = ZY19.NUDOSS;
END;

