SET job.name 'UPPER UDF'
REGISTER hadooppractice.jar;

DEFINE UPPER pig.myudfs.UPPER();

A = LOAD '/data/input/emp.tsv' AS (empid:int, empname:chararray, deptid:int, skills:chararray);
B = FOREACH A GENERATE empid, UPPER(empname);
DUMP B;