SET job.name 'Employee Group';

REGISTER hadooppractice.jar;

DEFINE MEAN pig.myudfs.Mean();

A = LOAD '/data/input/emp3.tsv' AS (empid:int, empname:chararray, deptid:int, skill1:chararray, skill2:chararray, skill3:chararray, mark:int);
B = FOREACH A GENERATE deptid, mark;
C = GROUP B By deptid;
D = FOREACH C GENERATE $0, MEAN($1.mark);
STORE D INTO '/data/dept-mean';