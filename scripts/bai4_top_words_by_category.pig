-- BAI 4: Top 5 tu positive/negative theo tung category

raw = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, comment:chararray, category:chararray,
        aspect:chararray, sentiment:chararray);

lowered = FOREACH raw GENERATE
    LOWER(comment) AS comment, category, aspect, sentiment;

tokenized = FOREACH lowered GENERATE
    category, aspect, sentiment,
    TOKENIZE(comment) AS words;

words_flat = FOREACH tokenized GENERATE
    category, aspect, sentiment,
    FLATTEN(words) AS word;

stopwords = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/stopwords.txt'
    USING PigStorage('\n')
    AS (stopword:chararray);

joined = JOIN words_flat BY word LEFT OUTER, stopwords BY stopword;
filtered = FILTER joined BY stopwords::stopword IS NULL;

clean = FILTER (FOREACH filtered GENERATE
    words_flat::category  AS category,
    words_flat::sentiment AS sentiment,
    words_flat::word      AS word)
BY (word IS NOT NULL AND word MATCHES '[\\p{L}\\p{N}]+');

-- Top 5 tu POSITIVE theo category
pos_words   = FILTER clean BY sentiment == 'positive';
pos_grouped = GROUP pos_words BY (category, word);
pos_freq    = FOREACH pos_grouped GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(pos_words) AS freq;
pos_by_cat  = GROUP pos_freq BY category;
pos_top5    = FOREACH pos_by_cat {
    sorted = ORDER pos_freq BY freq DESC;
    top5   = LIMIT sorted 5;
    GENERATE group AS category, FLATTEN(top5.(word, freq));
};

STORE pos_top5 INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai4_positive_top5'
    USING PigStorage(',');

-- Top 5 tu NEGATIVE theo category
neg_words   = FILTER clean BY sentiment == 'negative';
neg_grouped = GROUP neg_words BY (category, word);
neg_freq    = FOREACH neg_grouped GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(neg_words) AS freq;
neg_by_cat  = GROUP neg_freq BY category;
neg_top5    = FOREACH neg_by_cat {
    sorted = ORDER neg_freq BY freq DESC;
    top5   = LIMIT sorted 5;
    GENERATE group AS category, FLATTEN(top5.(word, freq));
};

STORE neg_top5 INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai4_negative_top5'
    USING PigStorage(',');