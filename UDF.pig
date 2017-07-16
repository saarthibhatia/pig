-- Lower to Upper case
REGISTER /home/hadoop1/pigudf.jar
DEFINE ConvertLowerToUpper myudfs.UPPER();
bag1 = load '/home/hadoop1/lower.txt' using PigStorage() as (name:chararray);
bag2 = foreach bag1 generate ConvertLowerToUpper(name);
--ConvertLowerToUpper is our UDF
-- jars- usr/local/pig (3 jars)
dump bag2;

