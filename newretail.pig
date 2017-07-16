data2000 = load '/home/hadoop1/2000.txt' USING PigStorage(',') AS (id, name, jan1:double, feb1:double, mar1:double, apr1:double, may1:double, jun1:double, jul1:double, aug1:double, sep1:double, oct1:double, nov1:double, dec1:double);

data2001 = load '/home/hadoop1/2001.txt' USING PigStorage(',') AS (id, name, jan2:double, feb2:double, mar2:double, apr2:double, may2:double, jun2:double, jul2:double, aug2:double, sep2:double, oct2:double, nov2:double, dec2:double);

data2002 = load '/home/hadoop1/2002.txt' USING PigStorage(',') AS (id, name, jan3:double, feb3:double, mar3:double, apr3:double, may3:double, jun3:double, jul3:double, aug3:double, sep3:double, oct3:double, nov3:double, dec3:double);

col_3_2000 = foreach data2000 generate $0, $1, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
--dump col_3_2000;
col_3_2001 = foreach data2001 generate $0, $1, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
col_3_2002 = foreach data2002 generate $0, $1, ($2+$3+$4+$5+$6+$7+$8+$9+$10+$11+$12+$13);
--dump col_3_2000;
joined = join col_3_2000 by $0, col_3_2001 by $0, col_3_2002 by $0;
--dump joined;
arrangedjoin = foreach joined generate $0, $1, $2, $5, $8;
--dump arrangedjoin;
addgrowth = foreach arrangedjoin generate $0, $1, $2, $3, $4, ROUND_TO(((($3-$2)*100)/$2),2) as g1, ROUND_TO(((($4-$3)*100)/$3),2) as g2;
--dump addgrowth;
average = foreach addgrowth generate $0, $1, $2, $3, $4, $5, $6, ROUND_TO((($5+$6)/2),2) as avg;
--dump average;
growthmorethan10 = filter average by avg>10;
--dump growthmorethan10;
orderaverage = order average by $7 desc;
--dump orderaverage;
rankedaverage = rank orderaverage;
--dump rankedaverage;
-- we got serial no. in 1st col
--describe rankedaverage;
filterrank = filter rankedaverage by rank_orderaverage>=10 and rank_orderaverage<=17;
--dump filterrank;
-- remove rank col
final = foreach filterrank generate $1,$2, $3, $4, $5, $6, $7, $8;
dump final;

