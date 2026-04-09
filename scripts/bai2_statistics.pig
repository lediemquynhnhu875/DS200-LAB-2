-- BAI 2: Thong ke
-- Schema: id;comment;category;aspect;sentiment

raw = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, comment:chararray, category:chararray,
        aspect:chararray, sentiment:chararray);

lowered = FOREACH raw GENERATE
    id, LOWER(comment) AS comment,
    category, aspect, sentiment;

tokenized = FOREACH lowered GENERATE
    id, category, aspect, sentiment,
    TOKENIZE(comment) AS words;

words_flat = FOREACH tokenized GENERATE
    id, category, aspect, sentiment,
    FLATTEN(words) AS word;

stopwords = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/stopwords.txt'
    USING PigStorage('\n')
    AS (stopword:chararray);

joined = JOIN words_flat BY word LEFT OUTER, stopwords BY stopword;

filtered = FILTER joined BY stopwords::stopword IS NULL;

clean = FOREACH filtered GENERATE
    words_flat::id        AS id,
    words_flat::category  AS category,
    words_flat::aspect    AS aspect,
    words_flat::sentiment AS sentiment,
    words_flat::word      AS word;

-- 2a: Tan so tung tu, chi lay > 500 lan
word_grouped = GROUP clean BY word;
word_freq = FOREACH word_grouped GENERATE
    group AS word,
    COUNT(clean) AS freq;
word_freq_500 = FILTER word_freq BY freq > 500;
word_freq_sorted = ORDER word_freq_500 BY freq DESC;

STORE word_freq_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai2_word_freq'
    USING PigStorage(';');

-- 2b: So binh luan theo category
distinct_id_cat = DISTINCT (FOREACH clean GENERATE id, category);
cat_grouped = GROUP distinct_id_cat BY category;
cat_count = FOREACH cat_grouped GENERATE
    group AS category,
    COUNT(distinct_id_cat) AS num_comments;
cat_sorted = ORDER cat_count BY num_comments DESC;

STORE cat_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai2_by_category'
    USING PigStorage(';');

-- 2c: So binh luan theo aspect
distinct_id_asp = DISTINCT (FOREACH clean GENERATE id, aspect);
asp_grouped = GROUP distinct_id_asp BY aspect;
asp_count = FOREACH asp_grouped GENERATE
    group AS aspect,
    COUNT(distinct_id_asp) AS num_comments;
asp_sorted = ORDER asp_count BY num_comments DESC;

STORE asp_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai2_by_aspect'
    USING PigStorage(';');