# CS-675 Final Project: Big Data Analytics at Cloud Scale

This capstone is completed individually.

## Guidelines

- Primary deliverable: a public **GitHub repository** (code, infrastructure, README, run instructions), committed incrementally.
- **July 25:** your scope and plan (datasets, questions, cloud-setup plan, and the extensions you will attempt).
- **August 1:** a short slide deck and a live demo of your stack, results, and design choices.
- This is an individual project: your submission must be entirely your own work.
- **Any code duplicated or shared between students will result in a zero.**
- AI assistants are allowed for learning and building, but you must justify every design choice in the demo; unexplained work scores low.
- Use at least two tools introduced in this course.
- Tear down all cloud resources when the project concludes.

## Tooling and setup

- Use an agentic AI IDE such as Cursor or Windsurf.
- Project setup guide: [`cloud-starter/AWS-SETUP.md`](../cloud-starter/AWS-SETUP.md).
- AWS: https://aws.amazon.com/getting-started/
- Terraform: https://developer.hashicorp.com/terraform/tutorials/aws-get-started

## Timeline

- **Saturday, July 18 - Project explanation and walkthrough.** The project and the [starter](../cloud-starter/README.md) are explained.
- **Saturday, July 25 - Scope review.** Present your datasets, questions, and cloud plan (5 minutes).
- **Saturday, August 1 - Final demo.** Demonstrate your results and answer questions (max 7 minutes).

## Steps

Work through the steps in order.

### Step 00 - The Spark solution

Goal: build a complete Spark analytics solution and get it working locally, using the course repo's dockerized Spark environment ([`code-starter/`](../code-starter/README.md)) on smaller slices of your data so you can iterate quickly.

- Preprocess with a rigorous pipeline: imputation, outlier treatment, normalization, encoding, and binning, with before-and-after justification.
- Write analytical queries that join across your data sources.
- Evaluate and discuss your results.

### Step 01 - The Spark solution, deployed in the cloud

Goal: run the same solution on cloud infrastructure at full scale, then package and present it.

- Deploy the same solution on the cloud (storage and a query or compute engine), reproducible from your repo.
- Run it at full scale (at least 100 million rows) on at least two joinable datasets.
- Package as a public GitHub repo with a README.
- Present your plan and results.

### Step 02 - Extensions (optional)

Goal: strengthen the project with extra big-data capabilities for bonus credit.

- Machine learning model development.
- Terabyte-scale data.
- Real-time data ingestion.
- A user interface or dashboard.
- Query-performance tuning (restructuring or denormalization, partitioning, and other big-data techniques).

## Score distribution

Each optional extension is worth 10 points. This distribution is provisional and may be adjusted; the required components currently total more than 100, which we will discuss.

**Required**

| Component | Step | Points |
|-----------|------|--------|
| Preprocessing pipeline | 00 | 10 |
| Cross-source join analytics | 00 | 20 |
| Cloud infrastructure | 01 | 20 |
| Big data at real scale (at least 100 million rows) | 01 | 16 |
| Code quality, repository organization, README and run instructions | 01 | 10 |
| Plan review | 01 | 10 |
| Results evaluation and discussion, and final presentation with Q&A | 00-01 | 20 |
| **Total** |  | **106** |

**Optional extensions - 10 points each (Step 02)**

| Extension | Points |
|-----------|--------|
| Machine learning model | 10 |
| Terabyte-scale data | 10 |
| Real-time ingestion | 10 |
| User interface or dashboard | 10 |
| Query-performance tuning | 10 |

## Datasets

Real-scale, joinable public datasets to consider:

- NYC TLC trip records (full history): billions of taxi and for-hire trips over multiple years.
- GH Archive (GitHub events): every public GitHub event over multiple years.
- Wikipedia pageviews and clickstream: billions of rows of page-view and navigation data.
- NOAA global weather (GSOD or GHCN-Daily): decades of daily readings worldwide.
- Common Crawl (one monthly slice via Athena): a web-scale crawl.
- A large reviews dataset (Yelp or Amazon style): tens of millions of reviews with text and ratings.
- Or propose a large, cloud-hosted dataset of your own (prior approval required).

Pick data large enough for the cloud but with a smaller slice (a date range, region, or sample) you can run locally first; the same analysis works at both scales.

## Cloud environment

- The [starter](../cloud-starter/README.md) (Terraform and a `Makefile`) provisions your S3 storage, an Athena workgroup for SQL, and optionally EMR Serverless (Spark) or a browser PySpark notebook.
- Learn the cloud environment and set up your account with the self-start guide, [`cloud-starter/AWS-SETUP.md`](../cloud-starter/AWS-SETUP.md); your workspace is [`cloud-starter/student-workspace/`](../cloud-starter/student-workspace/Readme.md).
- Getting an account: create and use your own AWS account (recommended); or use a user account the instructor provides on July 18; or ask the department about an education or license account.
- Manage cost: prefer Athena, stay within budget, and `make destroy` when finished.
