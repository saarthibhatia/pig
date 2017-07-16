wc = load '$input' using TextLoader() as (lines:chararray);
--dump wc;
transform = foreach wc generate FLATTEN(TOKENIZE(lines)) as word;
--dump transform;
transform = foreach transform generate TRIM(LOWER(REPLACE(word,'[\\p{Punct},\\p{Cntrl}]',''))) as word;
--dump transform;
--transform = filter transform by word == '$myword';
groupbyword = group transform by word;
--dump groupbyword;
--describe groupbyword;
countofeachword = foreach groupbyword generate group, COUNT(transform);
dump countofeachword;

