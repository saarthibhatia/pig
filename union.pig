book1 = load '/home/hduser/DATA/book1.txt' using PigStorage() as (lines:chararray);
book2 = load '/home/hduser/DATA/book2.txt' using PigStorage() as (lines:chararray);
bookcombined = union book1, book2;
dump bookcombined;
--split bookcombined into book3 if SUBSTRING(lines,5,7) =='is';
--dump book3;

