# Step-by-step runbook

Follow these steps in order. Each step says **what to do**, **how to check it worked**, and **what to save for the submission**.
Tick them off as you go. Estimated hands-on time: 5–6 hours.

> **Windows users:** do everything inside **WSL2 (Ubuntu)**. Spark on plain Windows needs extra setup (winutils) that isn't worth the time.

---

## Suggested 5-day plan

| Day | Steps | Goal |
|---|---|---|
| 1 | 0 – 4 | Accounts, tools, AWS resources, Polaris running, unit tests green |
| 2 | 5 – 7 | Data in S3, Iceberg tables built, Iceberg features done |
| 3 | 8 – 9 | DQ checks pass, Snowflake reads the tables, queries exported |
| 4 | 10 – 12 | Streamlit app live, README finished, repo pushed, rehearse |
| 5 | 13 | Email the links (keep a buffer day for problems) |

---

## Step 0 – Accounts and tools (≈45 min)

1. **AWS account.** Sign in as root **once** to create an IAM admin:
   IAM → Users → Create user `admin-yourname` → attach `AdministratorAccess` → create an access key (CLI) for it.
   From now on never use root. Turn on MFA for root.
2. **Snowflake free trial** at signup.snowflake.com → Edition *Enterprise* or *Standard* → Cloud **AWS** →
   Region **the same as your bucket** (e.g. US East (N. Virginia) = `us-east-1`). Same region avoids data-transfer cost.
3. **GitHub** account and a new **public** repo (or private, shared with the reviewer), e.g. `nyc-taxi-iceberg-pipeline`.
4. Install tools and check versions:
   ```bash
   python3.11 --version     # 3.11.x (PySpark 3.5 is happiest on 3.10/3.11)
   java -version            # 17 (e.g. Temurin 17). Spark 3.5 + Java 21 can break.
   docker --version && docker compose version
   aws --version            # aws-cli/2.x
   git --version && make --version
   ```
5. Configure the admin profile: `aws configure --profile my-admin` (paste the admin key, region `us-east-1`).
   Check: `aws sts get-caller-identity --profile my-admin` shows `...:user/admin-yourname` (not `:root`).

## Step 1 – Put the code in your repo (≈15 min)

```bash
git clone https://github.com/<you>/nyc-taxi-iceberg-pipeline.git
cd nyc-taxi-iceberg-pipeline
# copy all files from this project into the folder
python3.11 -m venv .venv && source .venv/bin/activate
make install
make test                 # all 24 tests should pass, no AWS needed
git add . && git commit -m "Project skeleton, transforms and unit tests" && git push
```

✅ **Check:** `24 passed`. 💾 Commit early and often: reviewers like to see a real commit history.

## Step 2 – Configure `.env` (≈5 min)

```bash
cp .env.example .env
```
Edit `.env`:
* `S3_BUCKET` → globally unique, lowercase, e.g. `harsha-nyc-taxi-lake-2025`
* `AWS_REGION` → same region as Snowflake
* `POLARIS_ROOT_CLIENT_SECRET` and `POLARIS_CREDENTIAL` → pick a local password (same in both)

Load it into your terminal (do this in every new terminal so `$S3_BUCKET` etc. work):
```bash
set -a; source .env; set +a
```

Confirm `.env` is ignored: `git status` must **not** list `.env`.

## Step 3 – Create AWS resources with least privilege (≈20 min)

```bash
make aws-setup ADMIN_PROFILE=my-admin
```
This creates the private, encrypted bucket, two IAM users and two IAM roles (see `infra/policies/`).
Copy the two ARNs it prints into `.env` (`POLARIS_STORAGE_ROLE_ARN`, and keep `SNOWFLAKE_ROLE_ARN` for step 9).

Create access keys (never paste these into code or commit them):
```bash
# key for the pipeline (ingest + Spark) -> stored in a local AWS profile
aws iam create-access-key --user-name taxi-pipeline-user --profile my-admin
aws configure --profile taxi-pipeline        # paste that key/secret, region us-east-1

# key for the Polaris container -> into .env
aws iam create-access-key --user-name taxi-polaris-service-user --profile my-admin
#   put them in POLARIS_AWS_ACCESS_KEY_ID / POLARIS_AWS_SECRET_ACCESS_KEY in .env
```
✅ **Check least privilege works:**
```bash
AWS_PROFILE=taxi-pipeline aws s3 ls s3://$S3_BUCKET/          # works
AWS_PROFILE=taxi-pipeline aws s3 ls                           # AccessDenied (can't list all buckets) - good!
```
💾 Screenshot of the IAM users/roles page is a nice extra for the call.

## Step 4 – Start Apache Polaris (≈15 min)

```bash
make polaris-up            # docker compose up -d polaris
docker logs -f polaris     # wait for "Listening on: http://0.0.0.0:8181", then Ctrl+C
make polaris-bootstrap     # creates catalog taxi_catalog -> s3://<bucket>/warehouse
```
✅ **Check:** the script prints the catalog JSON with your bucket in `default-base-location`.

> **If Polaris won't work after ~45 minutes, switch to the Glue fallback** (allowed by the brief):
> set `CATALOG_TYPE=glue` in `.env`, re-run `make aws-setup ADMIN_PROFILE=my-admin` (adds Glue permissions), skip this step.
> Write the switch and the reason in the README assumptions.

## Step 5 – Part 1: Ingest (≈10 min)

```bash
make ingest
make ingest                # run it twice on purpose
aws s3 ls s3://$S3_BUCKET/raw/ --recursive --profile taxi-pipeline
```
✅ **Check:** first run logs `UPLOAD`, second logs `SKIP ... same sha256`. Exactly two objects in `raw/`.
💾 Save the terminal output of the second run (proof of idempotency).

## Step 6 – Part 2 + 3.1: Transform into Iceberg (≈20 min, first run downloads Spark jars)

```bash
make transform
```
✅ **Check the log:**
* `raw=… clean=… rejected=… (raw == clean + rejected: True)`
* a breakdown of rejected rows by reason
* `lake.nyc_taxi.trips_clean now has N rows`

Run `make transform` **again**: N must be identical → MERGE INTO is idempotent. 💾 Save both outputs.

Look at the files Iceberg wrote:
```bash
aws s3 ls s3://$S3_BUCKET/warehouse/ --recursive --profile taxi-pipeline | head -30
```
You'll see `data/pickup_date=2025-01-01/...parquet` (partitions) and `metadata/*.metadata.json`, `snap-*.avro` (snapshots/manifests).

## Step 7 – Part 3.2–3.4: Iceberg features and metrics (≈15 min)

```bash
make features     # schema evolution + time travel -> results/iceberg_features_report.txt
make metrics      # daily_zone_metrics
```
✅ **Check the report:** "Data files before ALTER … identical: True", the time-travel lines, and a row count for each snapshot.

## Step 8 – Part 5: Data quality (≈10 min)

```bash
make dq
make test
```
✅ **Check:** every `error` check is `PASS`. `warn` checks may show some outliers. That's real-world TLC data, and you can explain it.
If an error check fails, read the detail column: it tells you how many rows break the rule.

## Step 9 – Part 4: Snowflake (≈60 min, the trickiest step)

1. Open Snowsight → Worksheets → paste `sql/snowflake_setup.sql`. Replace `<BUCKET>` and `<ACCOUNT_ID>`.
2. Run steps 1–2 (warehouse, database, external volume).
3. Run `DESC EXTERNAL VOLUME taxi_s3_vol;` → in the `STORAGE_LOCATION_1` row copy `STORAGE_AWS_IAM_USER_ARN`.
4. In your terminal, lock the AWS role's trust policy to Snowflake:
   ```bash
   make aws-setup ADMIN_PROFILE=my-admin \
     SNOWFLAKE_IAM_USER_ARN='arn:aws:iam::xxxx:user/xxxx' SNOWFLAKE_EXTERNAL_ID=taxi_iceberg_ext_id
   ```
5. Wait 30 seconds, then in Snowflake: `SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('taxi_s3_vol');` → `"success":true`.
6. Run step 4 (catalog integration).
7. In the terminal: `make snowflake-sql` → paste `build/snowflake_tables.sql` into Snowsight and run it.
8. `SELECT COUNT(*) FROM trips_clean;` must equal the Spark count from step 6.
9. Open `sql/queries.sql`, run each query, then **Download → CSV** with the file names in the comments into `results/`.
   Also take a screenshot of each result → `results/snowflake_q1.png` etc.

> Whenever you re-run Spark jobs later, run `make snowflake-sql` again and execute the `ALTER ICEBERG TABLE … REFRESH` lines.

**Common Snowflake errors**
| Error | Fix |
|---|---|
| `Error assuming AWS_ROLE` / access denied | Trust policy not updated (step 4), wrong external ID, or wait a minute for IAM |
| `invalid identifier PICKUP_ZONE` | Use double-quoted lowercase column names, as in `queries.sql` |
| `METADATA_FILE_PATH ... not found` | The path must be relative to `s3://<bucket>/warehouse/`; regenerate with `make snowflake-sql` |
| Verify fails on `list` | Change the `s3:prefix` condition in `snowflake-role-policy.json` to `["*"]` and re-run aws-setup |

## Step 10 – Working URL: Streamlit (≈20 min)

```bash
pip install -r app/requirements.txt
make app                    # check it locally at http://localhost:8501
git add results app && git commit -m "Query results and Streamlit app" && git push
```
Then share.streamlit.io → sign in with GitHub → **Create app** → your repo, branch `main`, file `app/streamlit_app.py` → Deploy.
Copy the URL into the README table. The app only reads CSVs, so no secrets are needed.

Optional extra: a ≤5-min Loom / YouTube (unlisted) recording of `make all` + the Snowflake queries.

## Step 11 – Finish the README (≈30 min)

* Fill in the Streamlit URL.
* Add 2–3 findings from the results (busiest hour, top revenue zone, card vs cash tips).
* Re-read **Assumptions** and **Known limitations**. Change anything you did differently.
* Secret scan before the final push:
  ```bash
  git ls-files | grep -E '\.env$' && echo "STOP: .env is tracked!"
  git grep -nE 'AKIA[0-9A-Z]{16}|aws_secret_access_key|BEGIN PRIVATE KEY' || echo "no secrets found"
  ```

## Step 12 – Rehearse (≈45 min)

Read `docs/INTERVIEW_PREP.md`. Make sure you can explain each file without notes, and practise the
"small live change" exercises there, with the timer running.

## Step 13 – Submit

Email **sandeep@yottasys.com** with the subject e.g. *"Junior Data Engineer take-home – Harsha Vardhan Alladi"*:
* Git repository URL
* Working URL (Streamlit app / recording / Google Doc with screenshots)

## After the review – clean up (required by the brief)

```bash
make aws-teardown ADMIN_PROFILE=my-admin     # bucket + IAM users/roles
make polaris-down
```
In Snowflake run the `DROP` statements the teardown script prints.
