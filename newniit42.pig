
txn = load '/home/hadoop1/txns1.txt' USING PigStorage(',') AS (txnid, date, custid, amount:double, category, product, city, state, type);
txnbygroup = group txn all;
--describe txnbygroup;
totalcount = FOREACH txnbygroup generate COUNT(txn);
dump totalcount;
-- in hive we will not write group all it is inbuilt but in pig we have to write group.
 
