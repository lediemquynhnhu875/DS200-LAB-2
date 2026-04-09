-- BAI 2: Thong ke
-- Schema: id;comment;category;aspect;sentiment

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

clean = FOREACH filtered GENERATE
    words_flat::category  AS category,
    words_flat::aspect    AS aspect,
    words_flat::sentiment AS sentiment,
    words_flat::word      AS word;

clean = FILTER clean BY (word IS NOT NULL AND SIZE(word) > 1);

-- 2a: Tan so tung tu > 500
word_grouped = GROUP clean BY word;
word_freq = FOREACH word_grouped GENERATE
    group AS word,
    COUNT(clean) AS freq;
word_freq_500 = FILTER word_freq BY freq > 500;
word_freq_sorted = ORDER word_freq_500 BY freq DESC PARALLEL 1;

STORE word_freq_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai2_word_freq'
    USING PigStorage(';');

-- 2b: So binh luan theo category
distinct_cat = DISTINCT (FOREACH clean GENERATE category, aspect, sentiment);
cat_grouped = GROUP distinct_cat BY category;
cat_count = FOREACH cat_grouped GENERATE
    group AS category,
    COUNT(distinct_cat) AS num_comments;
cat_sorted = ORDER cat_count BY num_comments DESC PARALLEL 1;

STORE cat_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai2_by_category'
    USING PigStorage(';');

-- 2c: So binh luan theo aspect
distinct_asp = DISTINCT (FOREACH clean GENERATE category, aspect, sentiment);
asp_grouped = GROUP distinct_asp BY aspect;
asp_count = FOREACH asp_grouped GENERATE
    group AS aspect,
    COUNT(distinct_asp) AS num_comments;
asp_sorted = ORDER asp_count BY num_comments DESC PARALLEL 1;

STORE asp_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai2_by_aspect'
    USING PigStorage(';');