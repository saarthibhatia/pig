gw = load '/home/hadoop1/gateway' USING PigStorage(',') AS (bank, successrate:double);
wb = load '/home/hadoop1/weblog' USING PigStorage(',') AS (name, bank, time);
newwb = foreach wb generate $0, $1;
joined = join gw by $0, newwb by $1;
--dump joined;
arrange = foreach joined generate $2, $1, $0;
--dump arrange;
--describe arrange;
grouparrange = group arrange by name;
--dump grouparrange;
--describe grouparrange;
avgofgr = foreach grouparrange generate group, AVG(arrange.successrate) as avg;
--dump avgofgr;
rankedavg = rank avgofgr;
dump rankedavg;

