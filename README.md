<div align="center">

<h1>🔄 Automated Dataflow Pipeline</h1>

<p>
  <strong>A production-grade, fully automated weekly ETL pipeline that scrapes, transforms, and loads hospitality & dining data from Egyptian cities into a cloud data warehouse — orchestrated by Apache Airflow and containerized with Docker.</strong>
</p>

<br/>

<!-- Badges -->
<p>
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Supabase-PostgreSQL-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white"/>
  <img src="https://img.shields.io/badge/Selenium-Chrome-43B02A?style=for-the-badge&logo=selenium&logoColor=white"/>
  <img src="https://img.shields.io/badge/Celery-Redis-37814A?style=for-the-badge&logo=celery&logoColor=white"/>
</p>

<p>
  <img src="https://img.shields.io/badge/Schedule-Weekly-orange?style=for-the-badge&logo=clockify&logoColor=white"/>
  <img src="https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=for-the-badge"/>
  <img src="https://img.shields.io/badge/Graduation-Project-blueviolet?style=for-the-badge"/>
</p>

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Pipeline Flow](#-pipeline-flow)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Model](#-data-model)
- [Setup & Installation](#-setup--installation)
- [Running the Pipeline](#-running-the-pipeline)
- [Key Engineering Challenges Solved](#-key-engineering-challenges-solved)
- [Screenshots](#-screenshots)

---

## 🌍 Overview

This project is a **fully automated, end-to-end data pipeline** designed to collect, process, and store hospitality and dining data from major Egyptian cities. It runs on a **weekly schedule** without any manual intervention.

The pipeline targets two major platforms:

| Source | Data Collected |
|--------|---------------|
| 🏨 **Booking.com** | Hotels — names, ratings, prices, room types, locations |
| 🍽️ **Talabat** | Restaurants — names, cuisines, ratings, delivery areas |

All collected data is cleaned, transformed into a structured star schema, and loaded into a **Supabase PostgreSQL** cloud database — ready for analytics and reporting.

> **Graduation Project** — Faculty of Computers & Information, Egypt  
> Built as a showcase of real-world data engineering practices including web scraping, ETL, workflow orchestration, containerization, and cloud data warehousing.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Stack                         │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────────┐ │
│  │    Redis     │    │  PostgreSQL  │    │   Selenium Chrome      │ │
│  │  (Celery     │    │  (Airflow    │    │   Standalone           │ │
│  │   Broker)    │    │   Metadata)  │    │   (Web Scraping)       │ │
│  └──────┬───────┘    └──────┬───────┘    └────────────┬───────────┘ │
│         │                  │                          │             │
│  ┌──────▼──────────────────▼──────────────────────────▼───────────┐ │
│  │                   Apache Airflow 2.9.1                         │ │
│  │   ┌────────────┐  ┌─────────────┐  ┌──────────┐  ┌─────────┐   │ │
│  │   │ Webserver  │  │  Scheduler  │  │  Worker  │  │Triggerer│   │ │
│  │   │ :8081      │  │             │  │ (Celery) │  │         │   │ │
│  │   └────────────┘  └─────────────┘  └──────────┘  └─────────┘   │ │
│  └─────────────────────────┬──────────────────────────────────────┘ │
│                            │ DAG Execution                          │
└────────────────────────────┼────────────────────────────────────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │           Weekly ETL DAG            │
          │                                     │
          │ [Scrape Booking] ──┐                │
          │                    ├─► [Transform]  │
          │ [Scrape Talabat] ──┘      │         │
          │                            ▼        │
          │                  [Load to Supabase] │
          └─────────────────────────────────────┘
                                       │
                          ┌────────────▼────────────┐
                          │   Supabase PostgreSQL   │
                          │  (Cloud Data Warehouse) │
                          └─────────────────────────┘
```

---

## 🔁 Pipeline Flow

The pipeline is orchestrated as an Airflow DAG that runs **every week** automatically:

```
START
  │
  ├─────────────────────────────────────────────────┐
  │                                                  │
  ▼                                                  ▼
[Task 1A]                                       [Task 1B]
Scrape Booking.com                           Scrape Talabat
(Hotels data for                             (Restaurants data for
 Egyptian cities)                             Egyptian cities)
  │                                                  │
  └──────────────────┬───────────────────────────────┘
                     │  (Run in Parallel)
                     ▼
                  [Task 2]
          Clean & Transform Data
           • Standardize columns
           • Handle nulls & types
           • Deduplicate records
           • Map to star schema
                     │
                     ▼
                  [Task 3]
        Load to Supabase PostgreSQL
          • Upsert with conflict handling
          • NaN-safe JSON serialization
          • Batch inserts per table
                     │
                     ▼
                    END
```

### Stage Details

**🕷️ Scraping**
- Selenium WebDriver connects to a remote Chrome instance (Selenium Grid)
- Supports up to 5 concurrent browser sessions
- Handles dynamic content, pagination, and lazy-loaded elements
- Session timeout set to 1 hour for long-running scrapes

**🔧 Transformation**
- Raw CSVs cleaned using pandas
- Column standardization and type casting
- Null handling via `json.loads(df.to_json())` pattern for NaN-safe serialization
- Data mapped to the star schema dimensions and fact tables
- `glob` wildcard patterns + `find_latest_file()` for robust file discovery

**☁️ Loading**
- `supabase-py` client for PostgreSQL upsert operations
- Conflict resolution with `on_conflict` handling
- Separate loaders for each dimension table and fact table

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestration** | Apache Airflow 2.9.1 | DAG scheduling, task dependency management |
| **Executor** | CeleryExecutor + Redis | Distributed task execution |
| **Containerization** | Docker + Docker Compose | Reproducible, isolated environment |
| **Web Scraping** | Selenium + Chrome | Dynamic page interaction & data extraction |
| **Selenium Grid** | `selenium/standalone-chrome` | Headless, multi-session browser pool |
| **Data Processing** | Python + pandas | Cleaning, transformation, schema mapping |
| **Data Warehouse** | Supabase (PostgreSQL) | Cloud-hosted relational database |
| **Metadata DB** | PostgreSQL 13 | Airflow internal state management |
| **Message Broker** | Redis 7.2 | Celery task queue |
| **Notebooks** | Jupyter | Exploratory analysis & prototyping |

---

## 📁 Project Structure

```
Automated_Dataflow_Pipeline/
│
├── dags/                          # Airflow DAG definitions
│   └── pipeline_dag.py            #   Main weekly ETL DAG
│
├── scraping_scripts/              # Web scraping modules
│   ├── booking_scraper.py         #   Booking.com hotel scraper
│   └── talabat_scraper.py         #   Talabat restaurant scraper
│
├── cleaning_Data/                 # Transformation logic
│   ├── clean_booking.py           #   Hotel data cleaner
│   └── clean_talabat.py           #   Restaurant data cleaner
│
├── database/
│   └── Data_files/                # Raw & staged CSV outputs
│
├── supabase/                      # Database loading scripts
│   └── load_to_supabase.py        #   Upsert logic per table
│
├── scripts/                       # Utility scripts
│
├── logs/                          # Airflow task logs
│
├── docker-compose.yaml            # Full Docker stack definition
├── .env                           # Environment variables
└── README.md
```

---

## 📊 Data Model

The data warehouse follows a **Star Schema** design:

```
                    ┌──────────────────┐
                    │   fact_trips     │
                    │──────────────────│
                    │  trip_id (PK)    │
                    │  hotel_id (FK)   │──────────► dim_hotels
                    │  room_id (FK)    │──────────► dim_room_hotel
                    │  restaurant_id   │──────────► dim_restaurants
                    │  place_id (FK)   │──────────► dim_places
                    │  ...metrics...   │
                    └──────────────────┘

┌──────────────────┐    ┌────────────────────┐    ┌──────────────────┐
│   dim_hotels     │    │   dim_room_hotel   │    │ dim_restaurants  │
│────────────────  │    │────────────────────│    │──────────────────│
│ hotel_id (PK)    │    │ room_id (PK)       │    │ restaurant_id(PK)│
│ name             │    │ hotel_id (FK)      │    │ name             │
│ city             │    │ room_type          │    │ cuisine          │
│ rating           │    │ price_per_night    │    │ rating           │
│ review_count     │    │ ...                │    │ delivery_area    │
│ ...              │    └────────────────────┘    │ ...              │
└──────────────────┘                              └──────────────────┘

                    ┌──────────────────┐
                    │   dim_places     │
                    │──────────────────│
                    │ place_id (PK)    │
                    │ city             │
                    │ region           │
                    │ country          │
                    └──────────────────┘
```

---

## ⚙️ Setup & Installation

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24+)
- [Docker Compose](https://docs.docker.com/compose/) (v2+)
- At least **4 GB RAM** and **10 GB disk** available
- A [Supabase](https://supabase.com/) project with connection credentials

### 1. Clone the Repository

```bash
git clone https://github.com/kirlosmagdy/Automated_Dataflow_Pipeline.git
cd Automated_Dataflow_Pipeline
```

### 2. Configure Environment Variables

Edit the `.env` file with your credentials:

```env
# Airflow
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.9.1

# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-or-service-role-key
```

### 3. Initialize Airflow

```bash
docker compose up airflow-init
```

### 4. Launch the Full Stack

```bash
docker compose up -d
```

This starts all services:

| Service | URL |
|---------|-----|
| Airflow Webserver | http://localhost:8081 |
| Selenium noVNC (live browser view) | http://localhost:7900 |
| Celery Flower (task monitor) | http://localhost:5555 |

### 5. Access Airflow UI

Navigate to **http://localhost:8081**  
Default credentials: `airflow` / `airflow`

---

## 🚀 Running the Pipeline

### Trigger Manually

In the Airflow UI, enable and trigger the `automated_dataflow_pipeline` DAG — or use the CLI:

```bash
docker compose exec airflow-webserver airflow dags trigger automated_dataflow_pipeline
```

### Automatic Weekly Schedule

The DAG is configured to run automatically every week. No manual action required once the stack is running.

### Monitor Execution

- **Airflow UI** → Graph View → watch tasks transition from `queued → running → success`
- **Selenium noVNC** → open `http://localhost:7900` to watch the Chrome browser scrape live
- **Logs** → available in the `logs/` directory and in the Airflow UI per task

---

## 🧠 Key Engineering Challenges Solved

| Challenge | Solution |
|-----------|----------|
| **Concurrent Selenium sessions failing** | Set `SE_NODE_MAX_SESSIONS=5` and `SE_NODE_OVERRIDE_MAX_SESSIONS=true` on the Chrome container |
| **Session timeout on long scrapes** | Increased `SE_NODE_SESSION_TIMEOUT` from 300s to 3600s (1 hour) |
| **Airflow worker starting before Chrome ready** | Added `selenium-chrome: condition: service_healthy` dependency on the worker service |
| **Heartbeat timeouts on long-running tasks** | Tuned Celery worker heartbeat and task timeout settings |
| **Date-based filename mismatches** | Replaced hardcoded date filenames with `glob` wildcard pattern + `find_latest_file()` utility |
| **NaN values breaking Supabase JSON inserts** | Used `json.loads(df.to_json(orient='records'))` pattern to auto-serialize NaN → null |
| **Parallel scraping coordination** | Booking and Talabat scrape tasks run concurrently before a shared transform step |

---

## 👤 Author

**Kirlos Magdy**  
Data Engineering Student | Egypt  

[![GitHub](https://img.shields.io/badge/GitHub-kirlosmagdy-181717?style=flat-square&logo=github)](https://github.com/kirlosmagdy)

---

<div align="center">
  <sub>Built with ❤️ as a graduation project — demonstrating real-world data engineering from raw web data to a queryable cloud warehouse.</sub>
</div>
