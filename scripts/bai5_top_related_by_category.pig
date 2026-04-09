-- BAI 5: Top 5 tu lien quan nhat theo category 

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
    words::word     AS word,
    words::category AS category;

-- Dem tan suat tung tu theo category
all_words_group = GROUP words_clean BY (category, word);
all_words_count = FOREACH all_words_group GENERATE
    group.category AS category,
    group.word     AS word,
    COUNT(words_clean) AS cnt;

-- Top 5 tu theo category
category_group = GROUP all_words_count BY category;
top5 = FOREACH category_group GENERATE
    group AS category,
    FLATTEN(TOP(5, 2, all_words_count)) AS (category_dummy, word, cnt);

final = FOREACH top5 GENERATE category, word, cnt;

STORE final INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai5_top5_related'
    USING PigStorage(',');