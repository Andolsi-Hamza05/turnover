CREATE TABLE Fact_Table (
    Fact_ID INT PRIMARY KEY,
    Absence_ID INT,
    Assignment_ID INT,
    Contract_ID INT,
    Employee_ID INT,
    Entry_Exit_ID INT,
    Status_ID INT,
    Measure1 INT,
    Measure2 DECIMAL(10, 2),
    ...
);

CREATE OR REPLACE PROCEDURE Populate_Fact_Table AS
BEGIN
    INSERT INTO Fact_Table (Fact_ID, Absence_ID, Assignment_ID, Contract_ID, Employee_ID, Entry_Exit_ID, Status_ID, Measure1, Measure2, ...)
    SELECT
        Fact_ID,
        a.Absence_Key,
        ass.Assignment_Key,
        c.Contract_Key,
        e.Employee_Key,
        ee.Entry_Exit_Key,
        s.Status_Key,
        Measure1,
        Measure2,
        ...
    FROM
        FactSourceData data
    JOIN
        Absence_Dim a ON data.absence_code = a.Absence_Code
    JOIN
        Assignment_Dim ass ON data.assignment_code = ass.Assignment_Code
    JOIN
        Contract_Dim c ON data.contract_code = c.Contract_Code
    JOIN
        Employee_Dim e ON data.employee_code = e.Employee_Code
    JOIN
        Entry_Exit_Dim ee ON data.entry_exit_code = ee.Entry_Exit_Code
    JOIN
        Status_Dim s ON data.status_code = s.Status_Code;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END Populate_Fact_Table;
