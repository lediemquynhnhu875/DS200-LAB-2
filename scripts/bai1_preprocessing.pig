-- BAI 1: Preprocessing
-- Schema: id;comment;category;aspect;sentiment

raw = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, comment:chararray, category:chararray,
        aspect:chararray, sentiment:chararray);

-- Buoc 1: Lowercase
lowered = FOREACH raw GENERATE
    id, LOWER(comment) AS comment,
    category, aspect, sentiment;

-- Buoc 2: Tokenize (tach tung tu)
tokenized = FOREACH lowered GENERATE
    id, category, aspect, sentiment,
    TOKENIZE(comment) AS words;

words_flat = FOREACH tokenized GENERATE
    id, category, aspect, sentiment,
    FLATTEN(words) AS word;

-- Buoc 3: Load stopwords
stopwords = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/stopwords.txt'
    USING PigStorage('\n')
    AS (stopword:chararray);

-- Loai bo stopword bang LEFT OUTER JOIN
joined = JOIN words_flat BY word LEFT OUTER, stopwords BY stopword;

filtered = FILTER joined BY stopwords::stopword IS NULL;

-- Bo cac tu rong
result = FILTER (FOREACH filtered GENERATE
    words_flat::id        AS id,
    words_flat::category  AS category,
    words_flat::aspect    AS aspect,
    words_flat::sentiment AS sentiment,
    words_flat::word      AS word)
BY (word IS NOT NULL AND SIZE(word) > 0);

STORE result INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai1' USING PigStorage(';');