# Bicycle Real-Time Intelligence — Hackathon Demo

A **complete, one-click deployable** Real-Time Intelligence (RTI) solution on Microsoft Fabric. This repo packages 26 Fabric items — from streaming eventstreams to ontology-backed AI agents — into a self-contained project that any teammate can deploy to their own workspace in under 15 minutes.

---

## Table of Contents

1. [The Problem](#the-problem)
2. [Why This Matters — Real-World Challenges](#why-this-matters--real-world-challenges)
3. [Our Solution — Fabric RTI + Medallion ETL](#our-solution--fabric-rti--medallion-etl)
4. [Solution Overview](#solution-overview)
5. [Architecture](#architecture)
6. [Task Flow](#task-flow)
7. [Quick Start — Deploy in 7 Steps](#quick-start--deploy-in-7-steps)
8. [Post-Deployment Steps](#post-deployment-steps)
9. [Item Inventory](#item-inventory)
10. [Data Model](#data-model)
11. [Notebooks Reference](#notebooks-reference)
12. [Pipeline](#pipeline)
13. [Semantic Models](#semantic-models)
14. [Data Agents](#data-agents)
15. [Real-Time Components](#real-time-components)
16. [Task Flow Import](#task-flow-import)
17. [Alternative — Local Python Scripts](#alternative--local-python-scripts)
18. [Repo Structure](#repo-structure)
19. [Troubleshooting](#troubleshooting)
20. [License](#license)

---

## The Problem

> **Real-world operations move in minutes, but decisions still happen on yesterday's data.**

Bike-sharing systems are booming in cities worldwide — London alone operates **800+ docking stations** serving millions of rides per year. Yet behind the scenes, operators face the same painful cycle every single day:

🚲 **Stations run empty** — riders arrive, find zero bikes, and leave frustrated. Every empty station is a missed ride, a lost fare, and a hit to customer satisfaction. During morning rush hour in London, popular stations like Waterloo or King's Cross can drain completely within 15 minutes.

🚛 **Trucks dispatched to wrong locations** — rebalancing vans carry bikes from full stations to empty ones, but dispatchers rely on stale spreadsheets or gut instinct. A truck sent to the wrong neighbourhood wastes 30-45 minutes of crew time, fuel, and road capacity — while the actual problem station stays empty.

🌧️ **Weather shifts demand — but no one acts until it's too late** — a sudden rainstorm can cut cycling demand by 40-60% in minutes. Conversely, an unexpected sunny afternoon drives demand spikes that overwhelm stations near parks and riverfronts. Operators only discover this after the fact, buried in next-day reports.

📋 **Reports arrive 24 hours later** — traditional BI dashboards refresh overnight. By the time a manager sees that Station X was critically empty yesterday at 8:30 AM, the same problem has already repeated this morning. Decisions are made on yesterday's data for today's problems.

### This Problem Is Universal

While this project is set in **London** (using real TfL Santander Cycles station data and Open-Meteo weather feeds), the underlying challenge applies to **any city, any fleet, any real-time operation**:

| City | System | Stations | Same Problems |
|------|--------|----------|---------------|
| London | Santander Cycles | 800+ | Empty stations during rush, weather-driven demand swings |
| New York | Citi Bike | 1,700+ | Rebalancing trucks can't keep up with commuter surges |
| Paris | Vélib' | 1,400+ | Morning/evening asymmetry leaves residential stations full, business districts empty |
| Barcelona | Bicing | 500+ | Tourist demand is weather-dependent and unpredictable |
| Toronto | Bike Share | 600+ | Seasonal extremes make static schedules useless |

The data changes. The pain stays the same: **you can't fix what you can't see in real time.**

---

## Why This Matters — Real-World Challenges

Bike-sharing operations sit at the intersection of several hard data problems:

### 1. Demand Is Hyper-Local and Hyper-Temporal

A station outside a train terminal behaves completely differently from one in a residential neighbourhood. Demand varies by:
- **Hour** — morning rush (07:00–09:00) vs. lunch vs. evening commute vs. late night
- **Day type** — weekday commuting patterns vs. weekend leisure rides
- **Season** — summer peaks can be 3-4x winter baselines
- **Weather** — rain, wind, temperature, and "feels-like" comfort all shift behaviour
- **Events** — football matches, concerts, and public holidays create unpredictable surges

No static report can capture this. You need **streaming data + ML forecasting + real-time alerting**.

### 2. Weather Is the Invisible Multiplier

Weather doesn't just affect whether people ride — it changes *where* and *when* they ride:

| Weather Condition | Impact on Demand | Operational Effect |
|-------------------|-----------------|--------------------|
| Heavy rain | -40% to -60% demand | Bikes pile up at destinations; origins stay full |
| Wind > 30 km/h | -20% to -35% demand | Leisure riders vanish; commuters still ride |
| Sudden sunshine after rain | +25% spike within 30 min | Stations near parks drain instantly |
| Temperature < 5°C | -30% baseline | Rebalancing trucks sit idle; waste of crew hours |
| Heat wave > 30°C | +15% tourism, -10% commuting | Demand shifts from business zones to waterfront |

Integrating weather data in real time — not as a next-day footnote — transforms reactive firefighting into **proactive fleet positioning**.

### 3. Rebalancing Is Expensive and Time-Sensitive

Every rebalancing truck run costs £15–£25 in direct costs (fuel, crew, wear). London's system may run **hundreds of truck dispatches per day**. If even 20% are suboptimal (wrong station, wrong time, wrong quantity), that's thousands of pounds wasted weekly.

The difference between a good rebalancing decision and a bad one is often **15 minutes of data freshness**. A report that's one hour stale is already too late.

### 4. Customer Experience Degrades Silently

Unlike a crashed website that triggers immediate alarms, an empty bike station just... sits there. Riders walk away. They don't file tickets — they just stop using the service. The operator sees a slow decline in ridership months later and can't pinpoint why.

Real-time station monitoring with automatic alerting catches these silent failures **as they happen**, not after the damage is done.

---

## Our Solution — Fabric RTI + Medallion ETL

This project solves every problem above using **Microsoft Fabric's Real-Time Intelligence** capabilities combined with a classic **Medallion Architecture** ETL pipeline:

```
  THE PROBLEM                          OUR SOLUTION
  ─────────────────────                ─────────────────────────────────────
  Stations run empty         ──────▶   Real-time availability monitoring
                                       via Eventstream + KQL Dashboard
                                       (seconds latency, not hours)

  Trucks go to wrong places  ──────▶   ML demand forecasting (Prophet)
                                       + priority-scored rebalancing
                                       recommendations per station

  Weather shifts demand      ──────▶   Live Open-Meteo weather feed
                                       joined to station data in Silver
                                       layer → cycling comfort index
                                       → weather-adjusted demand scores

  Reports arrive too late    ──────▶   Hot path: KQL Dashboard (seconds)
                                       Warm path: Direct Lake PBI (minutes)
                                       Alert path: Reflex → Teams (seconds)

  Can't ask questions        ──────▶   AI Data Agents with natural language
  about the data                       queries across SQL, DAX, and Graph
```

### How the Layers Work Together

| Layer | What It Does | Fabric Items | Latency |
|-------|-------------|-------------|--------|
| **Streaming Ingest** | Captures live bike station events + weather observations every 60 seconds | 2 Eventstreams, 1 Eventhouse, 1 KQL Database | **Seconds** |
| **Bronze (Raw)** | Stores raw JSON as-is for auditability and replay | 3 Lakehouses (bike, weather, bronze) | Minutes |
| **Silver (Clean)** | Deduplicates, validates, enriches with weather joins, adds time/date dimensions | Notebooks 03, 03a | Minutes |
| **Gold (Star Schema)** | 12 fact + dimension tables optimized for analytics; ML forecast outputs; station snapshots | Notebook 04, 06 | Minutes |
| **Semantic Layer** | 2 Semantic Models (Direct Lake) with 47+ DAX measures for self-service BI | Bicycle RTI Analytics, Bicycle Ontology Model | Refresh |
| **Ontology + Graph** | 12 entity types, 23 relationships — enables graph traversals and neighbourhood-scoped reasoning | Ontology, Graph Model | Refresh |
| **AI Agents** | Natural language interface: "Which stations need rebalancing?" routes to SQL/DAX/GQL automatically | 2 Data Agents + 1 Operations Agent | On-demand |
| **Alerts** | Reflex/Activator fires when utilization drops below threshold → Power Automate → Teams notification | 2 Reflexes | **Seconds** |

### What Makes This Different from a Standard BI Project

| Traditional Approach | This Project |
|---------------------|-------------|
| Nightly batch refresh | Streaming ingest every 60 seconds |
| Single Power BI report | Hot path (KQL) + warm path (PBI) + AI agents |
| No weather integration | Live weather feed with cycling comfort index |
| Manual rebalancing decisions | ML-scored priority recommendations per station |
| Static entity lookup | Ontology-backed graph model with 23 relationship traversals |
| Email-based alerts (if any) | Reflex → Power Automate → Teams in seconds |
| Ask an analyst | Ask the AI Agent in natural language |

### Built for London — Works Anywhere

The project uses **London Santander Cycles** station data and **Open-Meteo** weather APIs, but the architecture is location-agnostic:

- **Swap the station feed** — point the Eventstream at any GBFS-compliant bike-share API (Citi Bike, Vélib', Bicing, etc.)
- **Swap the weather feed** — Open-Meteo covers the entire globe; just change the lat/lon coordinates
- **Same medallion pipeline** — Bronze/Silver/Gold notebooks parameterize by station ID and location
- **Same ontology structure** — entity types (Station, Neighbourhood, WeatherObservation, etc.) are universal
- **Same agents** — the Data Agent instructions work with any city's data once the schema is populated

> **Bottom line:** This isn't just a London bike demo. It's a **reusable pattern** for any real-time fleet intelligence scenario — scooters, delivery vans, EV chargers, transit vehicles — wherever operations need to move faster than yesterday's report.

---

## Solution Overview

This project demonstrates a **full Real-Time Intelligence scenario** built on Microsoft Fabric. A fictional bike-sharing operator ("Bicycle Fleet") deploys sensor-equipped bikes across London. The solution:

- **Ingests** real-time bike station availability and live weather data via Eventstreams
- **Processes** data through a **Medallion Architecture** (Bronze → Silver → Gold)
- **Forecasts** hourly demand using ML models (Prophet / scikit-learn)
- **Models** fleet entities via an **Ontology** with 12 entity types and 23 relationship types
- **Visualizes** operations through a KQL Dashboard (hot-path) and Power BI Report (warm-path)
- **Reasons** over data using two **AI Data Agents** (natural language SQL + graph-backed ontology queries)
- **Activates** alerts through **Reflex/Activators** → Power Automate → Microsoft Teams

**Key Fabric capabilities demonstrated:**  
Lakehouse, Eventhouse, KQL Database, Eventstream, Notebook (PySpark), Data Pipeline, Semantic Model (Direct Lake), Report (PBIR), KQL Dashboard, Data Agent, Ontology, Graph Model, Reflex/Activator, ML Experiment

---

## Architecture

```
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                     Microsoft Fabric — "Bike Rental Hackathon" Workspace      │
 │                                                                               │
 │  ┌─────────────────┐        ┌───────────────┐                                │
 │  │  RTIbikeRental   │──┬───▶│  Eventhouse    │──▶ KQL Dashboard               │
 │  │  (Eventstream)   │  │    │  bikerental-   │    (Live Operations)            │
 │  └─────────────────┘  │    │  eventhouse     │                                │
 │                        │    └───────────────┘                                │
 │  ┌─────────────────┐  │                                                      │
 │  │  RTI-WeatherDemo │──┤                                                      │
 │  │  (Eventstream)   │  │                                                      │
 │  └─────────────────┘  │                                                      │
 │                        │    MEDALLION ARCHITECTURE                            │
 │                        │    ┌──────────┐  ┌──────────┐  ┌──────────┐         │
 │                        └───▶│  BRONZE   │─▶│  SILVER  │─▶│   GOLD   │         │
 │                             │  bikerental│  │  bicycles│  │  bicycles│         │
 │                             │  _bronze   │  │  _silver │  │  _gold   │         │
 │                             │  _raw      │  │          │  │          │         │
 │                             └──────────┘  └──────────┘  └────┬─────┘         │
 │                                                               │               │
 │                     ┌─────────────────────────────────────────┤               │
 │                     │                    │                    │               │
 │               ┌─────▼──────┐      ┌─────▼──────┐      ┌─────▼──────┐        │
 │               │  Analyze & │      │  Ontology   │      │  Realtime  │        │
 │               │    Train   │      │  12 entities │      │ Activation │        │
 │               │  (ML, KQL) │      │  23 rels     │      │ (Reflex)   │        │
 │               └─────┬──────┘      └─────┬──────┘      └─────┬──────┘        │
 │                     │                    │                    │               │
 │               ┌─────▼──────┐      ┌─────▼──────┐      ┌─────▼──────┐        │
 │               │  Semantic  │      │  Data Agent │      │  Power     │        │
 │               │  Models(2) │      │  (3 agents) │      │  Automate  │        │
 │               └─────┬──────┘      └────────────┘      └─────┬──────┘        │
 │                     │                                        │               │
 │               ┌─────▼──────┐                           ┌─────▼──────┐        │
 │               │  PBI Report│                           │  Teams     │        │
 │               │  (PBIR)    │                           │  Alerts    │        │
 │               └────────────┘                           └────────────┘        │
 └───────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

| Path | Flow | Latency |
|------|------|---------|
| **Hot path** | Eventstream → Eventhouse → KQL Dashboard | Seconds |
| **Warm path** | Eventstream → Bronze → Silver → Gold → Semantic Model → PBI Report | Minutes (pipeline) |
| **ML path** | Gold → Demand Forecast (Prophet / sklearn) → Gold tables | Pipeline run |
| **Ontology path** | Gold → Ontology entity tables → Graph Model → Data Agent | Pipeline run |
| **Alert path** | Eventstream → Reflex/Activator → Power Automate → Teams | Seconds |

---

## Task Flow

The Fabric **Task Flow** provides a visual overview of how all items relate in the workspace. This project includes a pre-built task flow definition matching the architecture above.

<details>
<summary><strong>Task Flow View (15 tasks, 15 connectors)</strong></summary>

| Task | Type | Items |
|------|------|-------|
| Raw streaming Bike Rentals | Get data | RTIbikeRental |
| Raw Streaming Weather | Get data | RTI-WeatherDemo |
| Eventhouse | Store data | bikerentaleventhouse |
| RTI Dashboard | Visualize data | KQL Dashboard |
| Bronze data | Store data | 2 Lakehouses, NB02, NB03a |
| Silver data | Store data | 1 Lakehouse, NB03, NB03a |
| Golden data | Store data | 1 Lakehouse, NB04 |
| Analyze and train | Analyze & train | NB05, NB06, NB08 |
| Semantic Model | General | 2 Semantic Models |
| Ontology | General | Ontology, Graph Model, NB09 |
| Realtime activation | Track data | 2 Activators |
| PowerBI Dashboard | Visualize data | PBI Report |
| Data Agent | General | 3 Data Agents |
| Power Automate Connector | General | (external) |
| Teams | General | (external) |

</details>

**To import the task flow:** See [Task Flow Import](#task-flow-import) below.

---

## Quick Start — Deploy in 7 Steps

### Prerequisites

- A Fabric workspace with **F64 or higher** capacity
- **Contributor** or higher permissions on the workspace

> **Choose your deployment path:**
>
> | Path | Best for | Auth needed |
> |------|----------|-------------|
> | **Path A: Fabric Notebook** (recommended) | Teams, shared environments | None — already signed in |
> | **Path B: Local Python** | CLI users, automation | Browser sign-in once |

---

### Path A: Fabric Notebook (Recommended — No Extra Auth)

#### Step 1: Clone the repo to your local machine

```bash
git clone https://github.com/kwamesefah_microsoft/RTI-Hackathon-Demo.git
```

#### Step 2: Upload workspace files to a staging Lakehouse

1. In your Fabric workspace, create a new **Lakehouse** (name it `deploy_staging`)
2. Open it → click **Upload → Upload folder**
3. Select the `workspace` folder from the cloned repo
4. Wait for upload to complete — you'll see ~26 sub-folders under `Files/workspace/`

#### Step 3: Upload and run the deploy notebook

1. Upload `Deploy_Bicycle_RTI.ipynb` from the cloned repo to your workspace
2. Open it → **attach** the `deploy_staging` lakehouse (left sidebar → Add lakehouse)
3. Run **Cell 1** — installs `fabric-cicd` (ignore pip warnings)
4. Run **Cell 2** — deploys all 23 items in 5 staged rounds. No GitHub, no PAT, no auth prompts.
5. Run **Cell 3** — auto-fixes KQL Dashboard query URI + removes broken Pipeline SM refresh activity
6. Run **Cell 4** — validates all items were created

#### Step 4: Start Eventstreams (data must flow first)

The eventstreams feed the **Bronze layer** — nothing downstream works without them.

1. Open **RTIbikeRental** Eventstream → confirm it's running (click **Start** if not)
2. Open **RTI-WeatherDemo** Eventstream → confirm it's running (click **Start** if not)
3. **Wait until you see data arriving** — open a Bronze Lakehouse and refresh; you should see rows in the tables
4. Let the streams run for 10–30 min to accumulate enough data for meaningful dashboards

> **Tip:** You can pause/stop the eventstreams once you have enough data to save capacity. Restart them anytime.

#### Step 5: Run the pipeline (Medallion processing)

The pipeline processes Bronze → Silver → Gold → ML → Ontology. **It requires Bronze data from Step 4.**

1. Open **PL_BicycleRTI_Medallion** in the workspace
2. Click **Run** (~15–25 minutes)
3. After pipeline completes, **manually refresh** both Semantic Models:
   - Open each model → click **Refresh now**
   - (Cell 3 removed the auto-refresh pipeline activity because it needs a connection that can't be created programmatically)

#### Step 6: Deploy Ontology + Graph + Operations Agent

Upload `Post_Deploy_Setup.ipynb` to your workspace and **Run all cells**. This creates:
- **Bicycle_Ontology_Model_New** — Ontology with 12 entity types, 23 relationship types
- **Bicycle_Ontology_Model_New_graph** — Graph Model linked to bicycles_gold lakehouse
- **Cycling-Campaign-Agent** — Operations Agent for campaign automation

Then: Open the **Graph Model** → click **Refresh now** (must be done after ontology creation AND after pipeline data loads).

#### Step 7: Verify everything is working

| Action | Where | Time |
|--------|-------|------|
| Open KQL Dashboard → confirm tiles show Eventhouse data | Fabric UI | 2 min |
| Test the Data Agent → ask *"Which stations need rebalancing?"* | Fabric UI | 1 min |
| *(Optional)* Delete the `deploy_staging` lakehouse | Fabric UI | 1 min |

#### You're done! 🎉

See [AGENT_SAMPLE_QUESTIONS.md](AGENT_SAMPLE_QUESTIONS.md) for 52 tested questions to try with the Data Agent.

---

### Path B: Local Python (Alternative)

If you prefer deploying from your local machine:

```bash
git clone https://github.com/kwamesefah_microsoft/RTI-Hackathon-Demo.git
cd RTI-Hackathon-Demo
pip install -r requirements.txt
python deploy.py
```

You'll be prompted for your workspace ID or name, then a browser window opens for Microsoft sign-in. After that, follow Steps 4–7 from Path A above.

---

## Post-Deployment Steps

After `Deploy_Bicycle_RTI.ipynb` (Cells 1–4) completes:

### Automated (via Post_Deploy_Setup.ipynb)

The `Post_Deploy_Setup.ipynb` notebook programmatically deploys 3 items that `fabric-cicd` doesn't support:

| Item | API Used | What It Does |
|------|----------|--------------|
| **Bicycle_Ontology_Model_New** | `POST /ontologies` + `updateDefinition` | Creates ontology, loads 75 definition parts (12 entity types, 12 data bindings, 23 relationships, 23 contextualizations), patches workspace/lakehouse GUIDs |
| **Bicycle_Ontology_Model_New_graph** | `POST /graphModels` + `updateDefinition` | Creates graph model with 4-part definition (graphType, dataSources, graphDefinition, stylingConfiguration) |
| **Cycling-Campaign-Agent** | `POST /items` | Creates operations agent with campaign instructions |

> **Ontology REST API**: The Fabric Ontology API (`/v1/workspaces/{id}/ontologies`) is documented at
> [learn.microsoft.com](https://learn.microsoft.com/en-us/rest/api/fabric/ontology/items).
> The `scripts/clients/ontology_client.py` wrapper covers create, getDefinition, updateDefinition, and delete.

> **Graph Model REST API**: The Fabric GraphModel API (`/v1/workspaces/{id}/graphModels`) is documented at
> [learn.microsoft.com](https://learn.microsoft.com/en-us/rest/api/fabric/graphmodel/items).
> The `scripts/clients/graph_client.py` wrapper covers create, getDefinition, updateDefinition, refresh, executeQuery, and getQueryableGraphType.

### Manual Steps

| Step | Action | Time |
|------|--------|------|
| **1. Start Eventstreams** | Open RTIbikeRental and RTI-WeatherDemo — click **Start** if not running. Wait for Bronze data. | 10–30 min |
| **2. Run Pipeline** | Open PL_BicycleRTI_Medallion → click **Run** (Bronze → Silver → Gold → ML → Ontology) | 15–25 min |
| **3. Refresh Semantic Models** | Open each Semantic Model → click **Refresh now** | 5 min |
| **4. Deploy Ontology + Graph** | Upload & run `Post_Deploy_Setup.ipynb` | 5 min |
| **5. Refresh Graph Model** | Open Graph Model in Fabric UI → click **Refresh now** (must be after ontology + pipeline data) | 5 min |
| **6. Verify KQL Dashboard** | Open KQL Dashboard → confirm tiles show data from Eventhouse | 2 min |
| **7. Test Data Agent** | Open Bicycle Fleet Intelligence Agent → ask: *"What are the top 5 busiest stations?"* | 1 min |
| **Import Task Flow** *(optional)* | See [Task Flow Import](#task-flow-import) — import `bicycle_rti_task_flow.json` | 5 min |
| **Configure Activators** *(optional)* | Create Reflex items manually, add alert triggers. See `docs/ACTIVATOR_SETUP.md` | 10 min |
| **Set up Power Automate** *(optional)* | Create flows triggered by Activator → send Teams notifications | 15 min |

---

## Item Inventory

### Automated Deployment (23 items via fabric-cicd)

| # | Item | Type | Description |
|---|------|------|-------------|
| 1 | `bikerental_bronze_raw` | Lakehouse | Raw bike station data landing zone |
| 2 | `weather_bronze_raw` | Lakehouse | Raw weather data landing zone |
| 3 | `bicycles_silver` | Lakehouse | Cleaned & enriched data |
| 4 | `bicycles_gold` | Lakehouse | Star schema dimensional model |
| 5 | `bikerentaleventhouse` | Eventhouse | KQL real-time analytics store |
| 6 | `bikerentaleventhouse` | KQLDatabase | Database within the Eventhouse |
| 7 | `02_Bronze_Streaming_Ingest` | Notebook | Streaming ingest to bronze lakehouses |
| 8 | `03_Silver_Enrich_Transform` | Notebook | Clean, validate, derive columns |
| 9 | `03a_Silver_Weather_Join` | Notebook | Join weather context to bike data |
| 10 | `04_Gold_Star_Schema` | Notebook | Build fact/dim tables |
| 11 | `05_KQL_Realtime_Queries` | Notebook | KQL query patterns and examples |
| 12 | `06_ML_Demand_Forecast` | Notebook | Prophet + sklearn demand forecasting |
| 13 | `07_Activator_Alerts` | Notebook | Activator alert configuration |
| 14 | `08_GeoAnalytics_HotSpots` | Notebook | Geographic demand heat maps |
| 15 | `09_Ontology_Neighbourhood_Filter` | Notebook | Ontology table population |
| 16 | `RTIbikeRental` | Eventstream | Sample bike data → Lakehouse + Eventhouse |
| 17 | `RTI-WeatherDemo` | Eventstream | Live weather (London) → Eventhouse |
| 18 | `Bicycle RTI Analytics` | SemanticModel | Direct Lake — fleet operations (10 tables) |
| 19 | `Bicycle Ontology Model` | SemanticModel | Direct Lake — entity relationships (12 tables) |
| 20 | `PL_BicycleRTI_Medallion` | DataPipeline | 5 activities: Bronze → Silver → Gold → ML → Ontology |
| 21 | `Bicycle Fleet Intelligence — Live Operations` | KQLDashboard | Real-time KQL visuals |
| 22 | `Bicycle Fleet Intelligence Agent` | DataAgent | NL→SQL across lakehouse + SM + graph |
| 23 | `ontology data agent` | DataAgent | Graph-backed ontology reasoning |

> **Not deployed automatically:** `Bicycle Fleet Operations Report` (Report — enhanced format, needs manual creation), `BicycleFleet_Activator` and `Cycling Campaign Activator` (Reflex — require manual alert rule configuration). See `docs/ACTIVATOR_SETUP.md`.

### Post-Deploy (3 items via Ontology & GraphModel REST APIs)

| # | Item | Type | API | Description |
|---|------|------|-----|-------------|
| 27 | `Bicycle_Ontology_Model_New` | Ontology | `/ontologies` + `/updateDefinition` | 12 entity types, 23 relationships, 12 data bindings, 23 contextualizations |
| 28 | `Bicycle_Ontology_Model_New_graph` | GraphModel | `/graphModels` + `/updateDefinition` | 4-part visual graph (graphType, dataSources, graphDefinition, styling) |
| 29 | `Cycling-Campaign-Agent` | OperationsAgent | `/items` | Campaign automation agent |

### External Integrations (manual setup)

| Integration | Purpose |
|-------------|---------|
| Power Automate | Flows triggered by Activator rules |
| Microsoft Teams | Alert notifications channel |

---

## Data Model

### Medallion Layers

```
BRONZE (raw)            SILVER (cleaned)         GOLD (star schema)
─────────────           ────────────────         ──────────────────
bikerental_bronze_raw   bicycles_silver          bicycles_gold
├── bikeraw_tb          ├── silver_stations      ├── fact_availability
├── weather_raw_tb      ├── silver_availability  ├── fact_hourly_demand
                        ├── silver_weather       ├── fact_rebalancing
                        ├── silver_joined        ├── fact_weather_impact
                                                 ├── dim_station
                                                 ├── dim_neighbourhood
                                                 ├── dim_date
                                                 ├── dim_time
                                                 ├── dim_weather
                                                 ├── dim_customers
                                                 ├── forecast_demand
                                                 ├── gold_station_snapshot
                                                 └── gold_availability_recent
```

### Star Schema

```
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌──────▼───────────────┐   ┌──────────────────┐
│ dim_station  │───│  fact_availability    │───│  dim_weather     │
└──────────────┘   │  fact_hourly_demand   │   └──────────────────┘
                   │  fact_rebalancing     │
┌──────────────┐   │  fact_weather_impact  │   ┌──────────────────┐
│dim_neighbour-│───│                       │───│  dim_time        │
│     hood     │   └───────────────────────┘   └──────────────────┘
└──────────────┘
```

### Ontology Entity Types (12)

| Entity | Description | Bound Table |
|--------|-------------|-------------|
| BicycleStation | Docking stations with GPS coordinates | dim_station |
| BicycleFleet | Collection of bikes at a station | fact_availability |
| Neighbourhood | Geographic districts | dim_neighbourhood |
| WeatherCondition | Temperature, wind, humidity | dim_weather |
| DemandForecast | ML-predicted hourly demand | forecast_demand |
| RentalTrip | Individual bike checkout/return | fact_hourly_demand |
| RebalancingEvent | Fleet redistribution actions | fact_rebalancing |
| WeatherImpact | Weather effect on ridership | fact_weather_impact |
| Customer | Rider profile | dim_customers |
| DatePeriod | Calendar hierarchy | dim_date |
| TimePeriod | Time-of-day hierarchy | dim_time |
| StationSnapshot | Point-in-time station status | gold_station_snapshot |

---

## Notebooks Reference

| Notebook | Stage | Key Operations |
|----------|-------|----------------|
| `02_Bronze_Streaming_Ingest` | Bronze | Reads eventstream output, writes to bikerental_bronze_raw |
| `03_Silver_Enrich_Transform` | Silver | Dedup, null handling, business rule validation, derived columns |
| `03a_Silver_Weather_Join` | Silver | Temporal join of weather data to station records |
| `04_Gold_Star_Schema` | Gold | Builds fact + dimension tables, aggregations, snapshots |
| `05_KQL_Realtime_Queries` | Analytics | KQL query patterns, anomaly detection, trend analysis |
| `06_ML_Demand_Forecast` | ML | Prophet time-series + sklearn regression, writes to forecast_demand |
| `07_Activator_Alerts` | Alerting | Configures Activator alert thresholds and conditions |
| `08_GeoAnalytics_HotSpots` | Analytics | H3 hex-grid hotspot detection, geographic demand patterns |
| `09_Ontology_Neighbourhood_Filter` | Ontology | Populates ontology-specific Gold tables + triggers SM refresh |

---

## Pipeline

**PL_BicycleRTI_Medallion** — Orchestrates the full ETL in 6 sequential activities:

```
NB03_Silver  →  NB03a_Weather  →  NB04_Gold  →  NB06_ML  →  NB09_Ontology  →  SM Refresh
   (Silver       (Weather          (Star           (Demand     (Ontology          (Bicycle
    Enrich)       Join)             Schema)         Forecast)   Tables)            Ontology
                                                                                   Model)
```

- **Run time:** ~15–25 minutes (first load) / ~5–10 minutes (incremental)
- **SM Refresh:** Refreshes `Bicycle Ontology Model` semantic model after ontology tables are populated
- **Schedule:** Configure in Fabric UI → Pipeline → Schedule (recommended: every 30 minutes)

---

## Semantic Models

### Bicycle RTI Analytics

- **Connection:** Direct Lake to `bicycles_gold` lakehouse
- **Tables:** 12 (4 facts + 6 dimensions + 2 snapshots)
- **Use case:** Fleet operations reporting — availability trends, demand patterns, weather impact
- **Consumed by:** Bicycle Fleet Operations Report (PBIR)

### Bicycle Ontology Model

- **Connection:** Direct Lake to `bicycles_gold` lakehouse
- **Tables:** 12 (mapped to ontology entity types)
- **Use case:** Graph-based entity exploration, ontology data agent queries
- **Consumed by:** Ontology data agent, Graph Model

---

## Data Agents

### Bicycle Fleet Intelligence Agent
The primary Data Agent with **three data sources**:
1. **Lakehouse tables** (`bicycles_gold`) — direct SQL over star schema
2. **Semantic model** (`Bicycle RTI Analytics`) — DAX queries
3. **Graph / Ontology** (`Bicycle_Fleet_Ontology`) — entity relationship traversal

Includes **44 few-shot examples** for natural language query understanding.

**Sample questions:**
- *"What are the top 5 busiest stations this week?"*
- *"Show me demand forecast for tomorrow"*
- *"Which neighbourhoods have the most rebalancing events?"*
- *"What is the weather impact on ridership?"*

### Ontology Data Agent
Graph-backed agent focused on **entity relationship reasoning**:
- *"What stations are in the Camden neighbourhood?"*
- *"Show all relationships for station X"*
- *"Which customers ride from weather-affected stations?"*

### Cycling-Campaign-Agent (Operations Agent)
Automated agent for **marketing campaign triggers** based on weather conditions and demand forecasts.

---

## Real-Time Components

### Eventstreams

| Eventstream | Source | Destinations | Key Fields |
|-------------|--------|-------------|------------|
| **RTIbikeRental** | Sample data ("Bicycles") | bikerental_bronze_raw (Lakehouse) + bikerentaldb (Eventhouse) | station_id, bikes_available, docks_available, timestamp |
| **RTI-WeatherDemo** | RealTimeWeather (London: 51.52, -0.04) | weather_events (Eventhouse) via ManageFields (18 renames) | temperature, wind_speed, humidity, weather_condition, timestamp |

### Eventhouse + KQL Database

- **bikerentaleventhouse** — stores hot-path streaming data
- **bikerentaldb** — KQL database with `bikeraw_tb` (bike data) and `weather_events` (weather data)
- **DatabaseSchema.kql** — included in the package; auto-deployed with KQL DB definition

### KQL Dashboard

**Bicycle Fleet Intelligence — Live Operations** — real-time dashboard with:
- Station occupancy heat map
- Bike availability time series
- Weather overlay panels
- Anomaly detection tiles

> After deployment, update the data source to point to your Eventhouse query URI.

### Reflex / Activators

| Activator | Trigger | Action |
|-----------|---------|--------|
| **BicycleFleet_Activator** | Bikes available < 3 OR demand spike > 150% | Power Automate → Teams alert |
| **Cycling Campaign Activator** | Sunny + temp > 20°C + weekend | Power Automate → campaign launch email |

> Activator shells are deployed; you must manually add rules in the Fabric UI.

---

## Task Flow Import

The project includes a **pre-built Task Flow** JSON definition that creates the visual architecture view shown in the workspace.

### How to Import

1. Open your workspace in the Fabric browser UI
2. Switch to **List View** (icon at top-left)
3. In the Task Flow area at the top, click **Import a task flow** (or the `⋯` menu → Import)
4. Select the file: [`task_flow/bicycle_rti_task_flow.json`](task_flow/bicycle_rti_task_flow.json)
5. After import, **assign items to tasks**:
   - Click each task card → click the 📎 (clip) icon
   - Select the items listed in the task flow definition
6. Run `python task_flow/deploy_task_flow.py` for a guided assignment checklist

### Task Flow Connectors

```
Raw Bike Data ──┬──▶ Eventhouse ──▶ RTI Dashboard
                │
Raw Weather  ───┤
                │
                └──▶ Bronze ──▶ Silver ──▶ Gold ──┬──▶ Analyze & Train ──▶ Semantic Models ──▶ PBI Report
                                                   │
                                                   ├──▶ Ontology ──▶ Data Agents
                                                   │
                                                   └──▶ Realtime Activation ──▶ Power Automate ──▶ Teams
```

> **Note:** Task flows are a workspace-level UI feature with no REST API. The JSON file is imported/exported manually through the Fabric UI. Item assignments are not preserved in the export — use `deploy_task_flow.py` for the mapping guide.

---

## Alternative — Local Python Scripts

For users who prefer deploying from a local machine instead of a Fabric notebook:

```bash
cd scripts/
cp .env.template .env
# Edit .env with your TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

pip install azure-identity requests python-dotenv

# Deploy step by step:
python 01_setup_fabric_resources.py    # Create lakehouses + eventhouse
python 02_deploy_notebooks.py          # Upload 9 notebooks
python 03_deploy_semantic_model.py     # Deploy 2 semantic models
python deploy_pipeline.py              # Deploy pipeline
python 04_deploy_rti_dashboard.py      # Deploy KQL dashboard
python 05_deploy_report.py             # Deploy PBI report
python 06_deploy_data_agent.py         # Deploy data agents

# Deploy Ontology + Graph Model (uses dedicated REST APIs)
python deploy_ontology.py              # Create ontology + push 75-part definition
python deploy_ontology.py --update     # Update existing ontology definition
python deploy_graph_model.py           # Deploy standalone graph model
python deploy_graph_model.py --update  # Update existing graph definition
```

### Ontology & Graph Model REST APIs

The `scripts/clients/` directory contains reusable API client wrappers:

| Client | API Base | Endpoints |
|--------|----------|-----------|
| `ontology_client.py` | `/v1/workspaces/{id}/ontologies` | list, get, create, getDefinition, updateDefinition, delete |
| `graph_client.py` | `/v1/workspaces/{id}/graphModels` | list, get, create, getDefinition, updateDefinition, refresh, executeQuery, getQueryableGraphType, delete |

**Known limitation:** Graph refresh (`POST /jobs/refreshGraph/instances`) returns `InvalidJobType` for ontology-managed (auto-provisioned) graphs. Only the Fabric UI **Refresh now** button works. Standalone graph models CAN be refreshed via API.

**Requirements:** Python 3.10+, Azure CLI authenticated (`az login`)

---

## Repo Structure

```
RTI-Hackathon-Demo/
├── Deploy_Bicycle_RTI.ipynb            # One-click Fabric launcher notebook
├── Post_Deploy_Setup.ipynb             # Ontology + Graph Model + Ops Agent
├── README.md                           # This file
│
├── workspace/                          # fabric-cicd format definitions (26 items)
│   ├── bikerental_bronze_raw.Lakehouse/
│   ├── bicycles_silver.Lakehouse/
│   ├── bicycles_gold.Lakehouse/
│   ├── weather_bronze_raw.Lakehouse/
│   ├── bikerentaleventhouse.Eventhouse/
│   ├── bikerentaleventhouse.KQLDatabase/
│   ├── 02_Bronze_Streaming_Ingest.Notebook/
│   ├── 03_Silver_Enrich_Transform.Notebook/
│   ├── 03a_Silver_Weather_Join.Notebook/
│   ├── 04_Gold_Star_Schema.Notebook/
│   ├── 05_KQL_Realtime_Queries.Notebook/
│   ├── 06_ML_Demand_Forecast.Notebook/
│   ├── 07_Activator_Alerts.Notebook/
│   ├── 08_GeoAnalytics_HotSpots.Notebook/
│   ├── 09_Ontology_Neighbourhood_Filter.Notebook/
│   ├── RTIbikeRental.Eventstream/
│   ├── RTI-WeatherDemo.Eventstream/
│   ├── Bicycle RTI Analytics.SemanticModel/
│   ├── Bicycle Ontology Model.SemanticModel/
│   ├── PL_BicycleRTI_Medallion.DataPipeline/
│   ├── Bicycle Fleet Operations Report.Report/
│   ├── Bicycle Fleet Intelligence — Live Operations.KQLDashboard/
│   ├── Bicycle Fleet Intelligence Agent.DataAgent/
│   ├── ontology data agent.DataAgent/
│   ├── BicycleFleet_Activator.Reflex/
│   └── Cycling Campaign Activator.Reflex/
│
├── post_deploy/
│   └── definitions/                    # Items unsupported by fabric-cicd
│       ├── Bicycle_Ontology_Model_New.Ontology/
│       │   ├── EntityTypes/            # 12 entities with DataBindings
│       │   └── RelationshipTypes/      # 23 relationships with Contextualizations
│       ├── Bicycle_Ontology_Model_New_graph.GraphModel/
│       └── Cycling-Campaign-Agent.OperationsAgent/
│
├── task_flow/
│   ├── bicycle_rti_task_flow.json      # Task flow definition (import via Fabric UI)
│   └── deploy_task_flow.py             # Validation + assignment guide
│
├── config/
│   └── deployment.yaml                 # fabric-launcher configuration
│
└── scripts/                            # Fallback local Python deploy scripts
    ├── .env.template
    ├── config.py
    ├── fabric_auth.py
    ├── 01_setup_fabric_resources.py
    ├── 02_deploy_notebooks.py
    ├── 03_deploy_semantic_model.py
    ├── 04_deploy_rti_dashboard.py
    ├── 05_deploy_report.py
    ├── 06_deploy_data_agent.py
    ├── deploy_pipeline.py
    ├── deploy_ontology.py              # Ontology REST API deployment
    ├── deploy_graph_model.py           # GraphModel REST API deployment
    └── clients/                        # Reusable Fabric API wrappers
        ├── ontology_client.py          #   POST /ontologies + updateDefinition
        └── graph_client.py             #   POST /graphModels + refresh + GQL query
```

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| **Eventstream not streaming** | Auto-start may not trigger | Open Eventstream → click **Start** |
| **Pipeline fails at NB04_Gold** | Silver tables don't exist yet | Run NB03 + NB03a manually first |
| **SM refresh fails in pipeline** | Connection ID is workspace-specific | Re-create SM refresh activity in pipeline editor |
| **KQL Dashboard shows no data** | Eventhouse query URI placeholder | Open Dashboard → Data sources → update URI to your Eventhouse |
| **Data Agent returns errors** | Ontology ID placeholder not replaced | After Ontology deploy, update datasource.json in Data Agent config |
| **Report is blank** | Semantic model not refreshed | Run pipeline or manually refresh `Bicycle RTI Analytics` SM |
| **Activator has no rules** | Shells deployed, rules not preserved | Add rules manually — see Reflex item in Fabric UI |
| **fabric-launcher fails** | Missing capacity or permissions | Ensure F64+ capacity and Contributor role |
| **Task flow import fails** | JSON format mismatch | Try creating tasks manually and use the import as a reference |

### Non-Resolvable Placeholders

After deployment, **6 placeholders** remain that need manual configuration:

| Placeholder | Where | How to Fix |
|-------------|-------|-----------|
| `__ONTOLOGY_FLEET__` | Data Agent datasource.json (3 occurrences) | Replace with Ontology item ID after Post_Deploy_Setup.ipynb |
| `__ONTOLOGY_NEW__` | Cycling Campaign Activator Reflex (3 occurrences) | Replace with Ontology item ID after Post_Deploy_Setup.ipynb |
| `__EVENTHOUSE_QUERY_URI__` | KQL Dashboard | Update data source in Dashboard editor |
| `__SM_REFRESH_CONN__` | Pipeline SM refresh activity | Re-create SM refresh connection in pipeline editor |

---

## License

Internal Microsoft demo — not for external distribution.
