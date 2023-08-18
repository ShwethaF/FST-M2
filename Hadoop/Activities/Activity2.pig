-- Load text file into Pig
inputData = LOAD 'hdfs:///user/shwetha/input.txt' AS (lines:chararray);
-- Tokenize lines into words
words = FOREACH inputData GENERATE FLATTEN(TOKENIZE(lines)) as word;
-- Group words by word
grpd = GROUP words by word;
-- Count the occurance of each word (REDUCE)
totalCount = FOREACH grpd GENERATE group, COUNT(words);
-- Remove the output file for reusability
rmf /user/shwetha/activity2ROutput;
-- Store output in HDFS
STORE totalCount INTO 'hdfs:///user/shwetha/activity2ROutput'; 