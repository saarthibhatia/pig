-- track cust whose age <50 and amt >500$
--steps
--1.join txn with cust
--2.group by txn table by custid
--3.sum total sales for each custid
--4.filter on above to get custid totalsales>500
--5.join this data with cust data
--6.filter on age col
txn = load '/home/hadoop1/txns1.txt' USING PigStorage(',') AS (txnid, date, custid, amount:double, category, product, city, state, type);
cust = load '/home/hadoop1/custs.txt' USING PigStorage(',') AS (custid, firstname, lastname, age:long, profession);
groupbycust = group txn by custid;
spendbycust = foreach groupbycust generate group, SUM(txn.amount) as total;
custmorethan500 = filter spendbycust by total>500;
--dump custmorethan500;
joined = join custmorethan500 by $0, cust by $0;
--dump joined;
agelessthan50 = filter joined by age < 50;
--dump agelessthan50;
final = foreach agelessthan50 generate $0, $3, $4, $5, $6, ROUND_TO($1,2);
--dump final;
--store final into '/home/hadoop1/niit43result' using PigStorage(',');
store final into '/home/hadoop1/niit43binaryresult/binary' using BinStorage();
