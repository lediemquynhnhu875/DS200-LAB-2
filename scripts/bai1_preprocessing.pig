-- BAI 1: Preprocessing
-- Schema: id;comment;category;aspect;sentiment

raw = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, comment:chararray, category:chararray,
        aspect:chararray, sentiment:chararray);

lowered = FOREACH raw GENERATE
    LOWER(comment) AS comment, category, aspect, sentiment;

cleaned1 = FOREACH lowered GENERATE
    REPLACE(comment, '[0-9,.&@!%+/\\\\?\\\'():>^~*#=\\[\\]{}]', '') AS comment,
    category, aspect, sentiment;

cleaned2 = FOREACH cleaned1 GENERATE
    REPLACE(comment, '-', '') AS comment,
    category, aspect, sentiment;

words = FOREACH cleaned2 GENERATE
    category, aspect, sentiment,
    FLATTEN(TOKENIZE(comment)) AS word;

stopwords = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/stopwords.txt'
    USING PigStorage()
    AS (stopword:chararray);

joined   = JOIN words BY word LEFT OUTER, stopwords BY stopword;
filtered = FILTER joined BY stopwords::stopword IS NULL;

words_clean = FOREACH filtered GENERATE
    words::category  AS category,
    words::aspect    AS aspect,
    words::sentiment AS sentiment;

STORE words_clean INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai1'
    USING PigStorage(',');