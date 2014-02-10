SET job.name 'Load UDF check';

REGISTER hadooppractice.jar;
DEFINE LOADEMP pig.myudfs.LoadEmpData();

A = LOAD '/data/input/emp0.tsv' USING LOADEMP(); 
-- AS (empid:int, empname:chararray, deptid:int, skill1:chararray, skill2:chararray, skill3:chararray, rank:int);
B = FOREACH A GENERATE empname, deptid;
C = LIMIT B 10;
DUMP C;