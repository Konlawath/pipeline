# Thammasat Data & AI Workshop — Pipeline

---

## โครงสร้างโปรเจค

```
pipeline/
├── config/
├── scripts/
│   └── batch_pipeline.py
├── sql/
│   └── 01_ddl.sql
├── output/
├── .env                          ← สร้างเองจาก .env.example
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── thammasat_workshop_dataset.xlsx
└── README.md
```

---

## ขั้นตอนการตั้งค่าโปรเจค

### 1. ตั้งค่าไฟล์ .env

คัดลอกไฟล์ `.env.example` แล้วสร้างเป็น `.env`

```bash
cp .env.example .env
```

แก้ไขค่าใน `.env` ให้ตรงกับที่ต้องการ

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=workshop_db
DB_USER=postgres
DB_PASSWORD=yourpassword
```

---

### 2. เริ่ม PostgreSQL ด้วย Docker

```bash
docker-compose up -d
```

ตรวจสอบว่า container รันอยู่

```bash
docker ps
```

ควรเห็น `postgres_workshop` status `Up`

---

### 3. สร้าง Tables (รันครั้งเดียว)

```bash
docker exec -i postgres_workshop psql -U postgres -d workshop_db -f sql/01_ddl.sql
```

ตรวจสอบว่า tables ถูกสร้างแล้ว

```bash
docker exec -it postgres_workshop psql -U postgres -d workshop_db -c "\dt workshop.*"
```

ควรเห็น 6 tables นี้

```
workshop.analytics_student_summary
workshop.batch_audit
workshop.raw_workshop_data
workshop.rag_excel_document_chunk
workshop.stg_student_snapshot
workshop.trusted_student_snapshot
```

---

### 4. ติดตั้ง Python Packages

```bash
pip install -r requirements.txt
```

---

### 5. วางไฟล์ Excel

วางไฟล์ `thammasat_workshop_dataset.xlsx` ไว้ใน root folder ของโปรเจค

```
pipeline/
└── thammasat_workshop_dataset.xlsx
```

---

## รัน Pipeline

```bash
python scripts/batch_pipeline.py \
  --business_date 2026-06-28 \
  --run_id RUN001 \
  --input_file thammasat_workshop_dataset.xlsx \
  --sheet_name workshop_data
```

Pipeline จะทำงาน 8 ขั้นตอนตามลำดับ

```
STEP 1 — อ่านไฟล์ Excel + คำนวณ MD5 checksum
STEP 2 — บันทึกข้อมูลดิบลง raw layer
STEP 3 — แปลง type ลง staging layer
STEP 4 — upsert ลง trusted layer
STEP 5 — upsert RAG corpus
STEP 6 — คำนวณ analytics summary
STEP 7 — quality check (count, sum, min, max)
STEP 8 — export CSV ทุก layer
```

เมื่อรันสำเร็จจะเห็น

```
✓ Pipeline completed successfully
Source: 180  Loaded: 180  Rejected: 0
```

---

## ทดสอบ Idempotency

รัน pipeline ซ้ำด้วย run_id ใหม่

```bash
python scripts/batch_pipeline.py \
  --business_date 2026-06-28 \
  --run_id RUN002 \
  --input_file thammasat_workshop_dataset.xlsx \
  --sheet_name workshop_data
```

ตรวจสอบว่า row count ไม่เพิ่ม

```bash
docker exec -it postgres_workshop psql -U postgres -d workshop_db \
  -c "SELECT COUNT(*) FROM workshop.trusted_student_snapshot;"
```

ควรได้ `180` เสมอ ไม่ว่าจะรันกี่ครั้ง

---

## ผลลัพธ์หลังรัน Pipeline

**Tables ใน PostgreSQL**

| Table | คำอธิบาย |
|---|---|
| `workshop.batch_audit` | log การรันทุกครั้ง |
| `workshop.raw_workshop_data` | ข้อมูลดิบทุก column |
| `workshop.stg_student_snapshot` | ข้อมูล type-cast แล้ว |
| `workshop.trusted_student_snapshot` | ข้อมูลสะอาด พร้อม query |
| `workshop.rag_excel_document_chunk` | chunks สำหรับ RAG |
| `workshop.analytics_student_summary` | summary aggregated |

**ไฟล์ใน `output/`**

```
output/
├── raw/        → raw_2026-06-28_RUN001.csv
├── staging/    → stg_2026-06-28_RUN001.csv
├── trusted/    → trusted_student_snapshot.csv
├── rag/        → rag_excel_document_chunk.csv  ← import เข้า JamAI ได้เลย
└── audit/      → batch_audit.csv, quality_summary_2026-06-28.json
```

---

## คำสั่ง SQL ที่ใช้บ่อย

```sql
-- ดู audit log
SELECT run_id, business_date, status, source_count, loaded_count
FROM workshop.batch_audit ORDER BY id DESC;

-- นักศึกษา active แยกตาม campus
SELECT campus, COUNT(*)
FROM workshop.trusted_student_snapshot
WHERE status = 'Active'
GROUP BY campus;

-- career interest top 5
SELECT career_interest, COUNT(*) AS cnt
FROM workshop.trusted_student_snapshot
GROUP BY career_interest
ORDER BY cnt DESC
LIMIT 5;

-- salary range
SELECT
  MIN(expected_salary_thb),
  MAX(expected_salary_thb),
  ROUND(AVG(expected_salary_thb), 0)
FROM workshop.trusted_student_snapshot;
```

---

## หยุดและลบ Docker Container

```bash
# หยุด
docker-compose down

# หยุดพร้อมลบข้อมูลใน database
docker-compose down -v
```