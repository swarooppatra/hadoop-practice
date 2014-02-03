SET job.name 'Employee Group';

REGISTER hadooppractice.jar;

DEFINE MEAN pig.myudfs.MeanV2('/data/input/detained.txt');

A = LOAD '/data/input/emp3.tsv' AS (empid:int, empname:chararray, deptid:int, skill1:chararray, skill2:chararray, skill3:chararray, mark:int);
B = FOREACH A GENERATE deptid, empname, mark;
C = GROUP B By deptid;
D = FOREACH C GENERATE $0, MEAN($1);
STORE D INTO '/data/dept-mean';