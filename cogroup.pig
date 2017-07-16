A = load '/home/hadoop1/purchase.txt' using PigStorage(',') as (prod:int, pqty:int);
B = load '/home/hadoop1/sales.txt' using PigStorage(',') as (prod:int, sqty:int);
-- prod as common key($0)
--C = cogroup A by $0, B by $0;
--(3 col are created $0, A(pqty), B(sqty)
-- cogroup can be run on multiple data together 
--cogrou
--p group means create ind gr and join(not union) then together
--dump C;
--D = foreach C generate group , SUM(A.pqty), SUM(B.sqty);
--dump D;

--prod id, total purchase qty, # of tran, total sales qty, # of sales trans
C = cogroup A by $0, B by $0;
D = foreach C generate group , SUM(A.pqty), COUNT(A), SUM(B.sqty), COUNT(B);
dump D;

