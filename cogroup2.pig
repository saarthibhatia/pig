A = load '/home/hadoop1/apparel.txt' using PigStorage(',') as (apid:int, type:chararray);
B = load '/home/hadoop1/person.txt' using PigStorage(',') as (name:chararray, apid:int);
C = cogroup A by $0, B by $01;
--dump C;
D = foreach C generate group, A.type, COUNT(B);
dump D;

