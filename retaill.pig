rd = load '/home/hadoop1/Retail Data' USING PigStorage(';') AS (time:chararray, cid:long, age, res, prodsub:long, prodid:long, qty, totalcost:long, totalsales:long);
--dump rd;
--describe rd;
--gr = group rd by cid;
--dump gr;
--describe gr;
d01filter = filter rd by STARTSWITH(time,'2001-01');
rt = foreach d01filter generate time, cid, totalsales;
--dump mx;
ordersale = order rt by $2 desc;
--dump ordersale;
rankedsale = rank ordersale;
filterrank = filter rankedsale by rank_ordersale<5;
dump filterrank;

