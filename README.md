# DS200 - LAB 2: Xử lý dữ liệu Hotel Review với Apache Pig

## 1. Cấu trúc thư mục

```
LAB-2/
├── data/
│   ├── hotel-review.csv        # Dữ liệu đánh giá khách sạn (11.770 dòng)
│   └── stopwords.txt           # Danh sách stop words tiếng Việt
├── scripts/
│   ├── bai1_preprocessing.pig          # Tiền xử lý dữ liệu
│   ├── bai2_statistics.pig             # Thống kê tần số & phân loại
│   ├── bai3_sentiment_aspect.pig       # Phân tích sentiment theo aspect
│   ├── bai4_top_words_by_category.pig  # Top 5 từ theo category & sentiment
│   └── bai5_tfidf_by_category.pig      # TF-IDF top 5 từ theo category
├── output/                      # Kết quả sau khi chạy 
│   ├── bai1/
│   ├── bai2_word_freq/
│   ├── bai2_by_category/
│   ├── bai2_by_aspect/
│   ├── bai3_negative/
│   ├── bai3_positive/
│   ├── bai4_positive_top5/
│   ├── bai4_negative_top5/
│   └── bai5_tfidf_top5/
├── evidence/                    # Ảnh minh chứng chạy từng bài
│   ├── 00_setup_pig.jpg         # Minh chứng cài đặt Apache Pig
│   ├── 01_bai1_result.png       
│   ├── 02_bai2_result.png       
│   ├── 03_bai3_result.png       
│   ├── 04_bai4_result.png       
│   └── 05_bai5_result.png       
├── .gitignore
└── README.md
```

---

## 2. Schema dữ liệu

File `hotel-review.csv` sử dụng dấu `;` làm delimiter, gồm 5 cột:

| Cột | Tên | Mô tả | Ví dụ |
|-----|-----|-------|-------|
| $0 | id | ID bình luận | 1 |
| $1 | comment | Nội dung bình luận | "Dịch vụ rất tốt..." |
| $2 | category | Phân loại đánh giá | GENERAL, QUALITY, STYLE&OPTIONS |
| $3 | aspect | Khía cạnh đánh giá | HOTEL, SERVICE, FOOD&DRINKS |
| $4 | sentiment | Nhãn cảm xúc | positive / negative |

---

## 3. Cài đặt môi trường

### Yêu cầu
- macOS / Linux
- Java 11+
- Apache Hadoop 3.x
- Apache Pig 0.17.0

### Cài đặt Java
```bash
brew install openjdk@11
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
java -version
```

### Cài đặt Hadoop
```bash
brew install hadoop
hadoop version
```

### Cài đặt Apache Pig
```bash
cd ~/Downloads
curl -O https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz
tar -xzf pig-0.17.0.tar.gz
sudo mv pig-0.17.0 /usr/local/pig

echo 'export PIG_HOME=/usr/local/pig' >> ~/.zshrc
echo 'export PATH=$PATH:$PIG_HOME/bin' >> ~/.zshrc
source ~/.zshrc

pig -version
```

---

## 4. Hướng dẫn chạy

### Tạo thư mục output
```bash
mkdir -p /Users/nhule/Desktop/UIT-DS-SJ/DS200/LAB-2/output
```

### Chạy từng bài

```bash
# Bài 1 - Tiền xử lý
pig -x local scripts/bai1_preprocessing.pig

# Bài 2 - Thống kê
pig -x local scripts/bai2_statistics.pig

# Bài 3 - Sentiment theo aspect
pig -x local scripts/bai3_sentiment_aspect.pig

# Bài 4 - Top 5 từ theo category & sentiment
pig -x local scripts/bai4_top_words_by_category.pig

# Bài 5 - TF-IDF theo category
pig -x local scripts/bai5_tfidf_by_category.pig
```

> ⚠️ Nếu chạy lại, xóa output cũ trước:
> ```bash
> rm -rf output/bai1 output/bai2_* output/bai3_* output/bai4_* output/bai5_*
> ```

---

## 📝 Mô tả từng bài

### Bài 1 — Tiền xử lý (`bai1_preprocessing.pig`)
- Chuyển toàn bộ văn bản về chữ thường (lowercase)
- Tách câu thành từng từ riêng lẻ (tokenize theo khoảng trắng)
- Loại bỏ stop words dựa vào `stopwords.txt`
- **Output:** `output/bai1/` — 181.204 records

### Bài 2 — Thống kê (`bai2_statistics.pig`)
- Thống kê tần số xuất hiện của các từ, lọc từ xuất hiện > 500 lần
- Thống kê số bình luận theo từng **category**
- Thống kê số bình luận theo từng **aspect**
- **Output:** `output/bai2_word_freq/`, `output/bai2_by_category/`, `output/bai2_by_aspect/`

### Bài 3 — Sentiment theo Aspect (`bai3_sentiment_aspect.pig`)
- Xác định aspect nhận nhiều đánh giá **negative** nhất
- Xác định aspect nhận nhiều đánh giá **positive** nhất
- **Output:** `output/bai3_negative/`, `output/bai3_positive/`

### Bài 4 — Top 5 từ theo Category & Sentiment (`bai4_top_words_by_category.pig`)
- Với mỗi category, tìm **5 từ xuất hiện nhiều nhất** trong bình luận positive
- Với mỗi category, tìm **5 từ xuất hiện nhiều nhất** trong bình luận negative
- **Output:** `output/bai4_positive_top5/`, `output/bai4_negative_top5/`

### Bài 5 — TF-IDF theo Category (`bai5_tfidf_by_category.pig`)
- Áp dụng thuật toán **TF-IDF** để tìm 5 từ liên quan nhất cho mỗi category
- TF-IDF đánh giá độ quan trọng của từ trong từng nhóm so với toàn bộ dataset
- **Output:** `output/bai5_tfidf_top5/`

---

## 5. Evidence

Ảnh minh chứng được lưu trong thư mục `evidence/`:

| File | Nội dung |
|------|----------|
| `00_setup_pig.jpg` | Terminal hiển thị `pig -version` thành công |
| `01_bai1_result.jpg` | Command bài 1 — danh sách từ sau khi tiền xử lý |
| `02_bai2_result.jpg` | Command bài 2 — thống kê từ, category, aspect |
| `03_bai3_result.jpg` | Command bài 3 — aspect positive/negative nhiều nhất |
| `04_bai4_result.jpg` | Command bài 4 — top 5 từ theo category & sentiment |
| `05_bai5_result.jpg` | Command bài 5 — top 5 từ TF-IDF theo category |
