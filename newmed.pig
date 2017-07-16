me = load '/home/hadoop1/medical' USING PigStorage(',') AS (name, dept, claim:double);
groupname = group me by name;
avgofgr = foreach groupname generate group, AVG(me.claim) as avg;
dump avgofgr;

