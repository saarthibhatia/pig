txn = load '/home/hadoop1/txns1.txt' USING PigStorage(',') AS (txnid, date, custid, amount:double, category, product, city, state, type);
--dump txn;
txnbytype = group txn by type;
-- whenever we do group we contain two col (group key,bag tuple)
--dump txnbytype;
totalbytype = FOREACH txnbytype generate group,ROUND_TO(SUM(txn.amount),2) as typesales;
-- using for each command we generate col group col or derived col 
--bagname=txn and nested col name =amount
--dump totalbytype;
--without writing a mapper we have to sum a values of cash and total sales
-- select prodit,sum(qty) from tbl group by prodit;
-- we know that sum fn works with group by
--so here we see how to add without mapper i.e. without group by
-- select sum(qty) from tbl>in pig
--in sql there is default mapper
-- so without writing  a mapper how can we sum in pig ??
-- so we need to create a mapper....
-- add cash+credit amount
totalgroup = group totalbytype all;
--dump totalgroup;
--(all,{(credit,4923134.93),(cash,187685.61)})
--describe totalgroup;
--totalgroup: {group: chararray,totalbytype: {(group: bytearray,typesales: double)}}
totalsales = FOREACH totalgroup generate SUM(totalbytype.typesales) as totalamt;
--dump totalsales;
final = FOREACH totalbytype generate $0, $1, ROUND_TO((($1*100)/totalsales.totalamt),2);
-- we can create many no. of col using generate command
dump final;
(cash,187685.61,3.67)
(credit,4923134.93,96.33)

