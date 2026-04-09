-- BAI 3: Khia canh nao co nhieu danh gia negative / positive nhat

SET pig.exec.mapPartAgg false;

raw = LOAD '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai1'
    USING PigStorage(',')
    AS (category:chararray, aspect:chararray, sentiment:chararray);

-- NEGATIVE
neg         = FILTER raw BY sentiment == 'negative';
neg_grouped = GROUP neg BY aspect;
neg_count   = FOREACH neg_grouped GENERATE
                group AS aspect,
                COUNT(neg) AS total;

neg_all   = GROUP neg_count ALL;
top_1_neg = FOREACH neg_all {
    result = TOP(1, 1, neg_count);
    GENERATE FLATTEN(result);
};

STORE top_1_neg INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai3_negative'
    USING PigStorage(',');

-- POSITIVE
pos         = FILTER raw BY sentiment == 'positive';
pos_grouped = GROUP pos BY aspect;
pos_count   = FOREACH pos_grouped GENERATE
                group AS aspect,
                COUNT(pos) AS total;

pos_all   = GROUP pos_count ALL;
top_1_pos = FOREACH pos_all {
    result = TOP(1, 1, pos_count);
    GENERATE FLATTEN(result);
};

STORE top_1_pos INTO '/Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output/bai3_positive'
    USING PigStorage(',');