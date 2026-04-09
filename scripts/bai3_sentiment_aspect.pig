-- BAI 3: Khia canh nao co nhieu danh gia negative / positive nhat
-- Schema: id;comment;category;aspect;sentiment

raw = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, comment:chararray, category:chararray,
        aspect:chararray, sentiment:chararray);

-- Lay distinct (id, aspect, sentiment) de tranh dem trung
distinct_rows = DISTINCT (FOREACH raw GENERATE id, aspect, sentiment);

-- NEGATIVE
neg = FILTER distinct_rows BY sentiment == 'negative';
neg_grouped = GROUP neg BY aspect;
neg_count = FOREACH neg_grouped GENERATE
    group AS aspect,
    COUNT(neg) AS neg_count;
neg_sorted = ORDER neg_count BY neg_count DESC;

STORE neg_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai3_negative'
    USING PigStorage(';');

-- POSITIVE
pos = FILTER distinct_rows BY sentiment == 'positive';
pos_grouped = GROUP pos BY aspect;
pos_count = FOREACH pos_grouped GENERATE
    group AS aspect,
    COUNT(pos) AS pos_count;
pos_sorted = ORDER pos_count BY pos_count DESC;

STORE pos_sorted INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai3_positive'
    USING PigStorage(';');