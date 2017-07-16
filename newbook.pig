bk = load '/home/hadoop1/book.txt' USING PigStorage(',') AS (bookid:long, price:double, authorid:long);
ath = load '/home/hadoop1/author.txt' USING PigStorage(',') AS (authorid:long, authorname);
filterbk = filter bk by price>=200;
--dump filterbk;
filterath = filter ath by INDEXOF(authorname,'J',0)==0;
dump filterath;
joininfo = join bk by $2, ath by $0;
--dump joininfo;

