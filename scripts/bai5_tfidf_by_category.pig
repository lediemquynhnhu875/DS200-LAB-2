-- BAI 5: Top 5 tu lien quan nhat theo category (TF-IDF)

raw = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, comment:chararray, category:chararray,
        aspect:chararray, sentiment:chararray);

lowered = FOREACH raw GENERATE
    id, LOWER(comment) AS comment, category;

tokenized = FOREACH lowered GENERATE
    id, category,
    TOKENIZE(comment) AS words;

words_flat = FOREACH tokenized GENERATE
    id, category,
    FLATTEN(words) AS word;

stopwords = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/stopwords.txt'
    USING PigStorage('\n')
    AS (stopword:chararray);

joined = JOIN words_flat BY word LEFT OUTER, stopwords BY stopword;

filtered = FILTER joined BY stopwords::stopword IS NULL;

clean = FOREACH filtered GENERATE
    words_flat::id       AS id,
    words_flat::category AS category,
    words_flat::word     AS word;

-- === TF: dem so lan tu xuat hien trong moi category ===
tf_grouped = GROUP clean BY (category, word);
tf = FOREACH tf_grouped GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(clean) AS tf;

-- === IDF: dem so category chua tu do ===
cat_word_distinct = DISTINCT (FOREACH clean GENERATE category, word);
idf_grouped = GROUP cat_word_distinct BY word;
idf = FOREACH idf_grouped GENERATE
    group AS word,
    COUNT(cat_word_distinct) AS doc_freq;

-- === Dem tong so category (N) ===
all_cats = DISTINCT (FOREACH clean GENERATE category);
all_cats_grouped = GROUP all_cats ALL;
N = FOREACH all_cats_grouped GENERATE
    COUNT(all_cats) AS total;

-- === JOIN TF va IDF ===
tf_idf_join = JOIN tf BY word, idf BY word;

-- === CROSS voi N ===
tf_idf_n = CROSS tf_idf_join, N;

-- === Tinh TF-IDF score ===
tfidf = FOREACH tf_idf_n GENERATE
    tf_idf_join::tf::category   AS category,
    tf_idf_join::tf::word       AS word,
    (double)tf_idf_join::tf::tf *
        LOG((double)N::total / (double)tf_idf_join::idf::doc_freq)
        AS tfidf_score;

-- === Top 5 theo category ===
tfidf_by_cat = GROUP tfidf BY category;
top5 = FOREACH tfidf_by_cat {
    sorted = ORDER tfidf BY tfidf_score DESC;
    top5   = LIMIT sorted 5;
    GENERATE group AS category, FLATTEN(top5);
};

STORE top5 INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai5_tfidf_top5'
    USING PigStorage(';');