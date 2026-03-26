# Ontology Manual Setup Guide

> **When to use this guide:** If the programmatic deployment (`Post_Deploy_Setup.ipynb` or
> `scripts/deploy_ontology.py`) fails or you prefer to build the ontology step-by-step
> in the Fabric UI. This guide walks through creating all 12 entity types, 23 relationships,
> data bindings, and keys manually from the `Bicycle Ontology Model` semantic model.

---

## Prerequisites

Before starting, ensure:

1. **Pipeline has run at least once** - `PL_BicycleRTI_Medallion` completes successfully
2. **`bicycles_gold` lakehouse exists** with all 12 ontology tables populated
3. **`Bicycle Ontology Model` semantic model** is deployed and refreshed

### Required Tables in `bicycles_gold`

| # | Table | Type | Row Estimate | Ontology Entity |
|---|-------|------|-------------|-----------------|
| 1 | `onto_dim_station` | Dimension | ~120 | Station |
| 2 | `dim_neighbourhood` | Dimension | 19 | Neighbourhood |
| 3 | `dim_time` | Dimension | 24 | TimeSlot |
| 4 | `dim_date` | Dimension | 365 | CalendarDate |
| 5 | `dim_weather` | Dimension | ~2,000 | WeatherObservation |
| 6 | `onto_fact_availability` | Fact | ~2,880 | AvailabilityEvent |
| 7 | `onto_fact_hourly_demand` | Fact | ~3,200 | HourlyDemand |
| 8 | `onto_fact_rebalancing` | Fact | varies | RebalancingAssessment |
| 9 | `onto_forecast_demand` | Fact | ~700 | DemandForecast |
| 10 | `onto_fact_weather_impact` | Fact | ~3,200 | WeatherImpact |
| 11 | `onto_station_snapshot` | Gold | ~120 | StationSnapshot |
| 12 | `onto_availability_recent` | Gold | ~2,880 | RecentAvailability |

> **Note:** The `onto_` prefix tables are created by `09_Ontology_Neighbourhood_Filter` notebook.
> Tables without the prefix (`dim_neighbourhood`, `dim_time`, `dim_date`, `dim_weather`) are
> shared with the main star schema.

---

## Step 1: Create the Ontology Item

1. Open your Fabric workspace
2. Click **+ New item** -> search for **Ontology** -> select it
3. Name: `Bicycle_Ontology_Model_New`
4. Click **Create**

The ontology opens in the Ontology editor.

---

## Step 2: Add Entity Types

For each entity below, click **+ Add entity type** and configure the properties.

### 2.1 - Station (Dimension)

| Setting | Value |
|---------|-------|
| **Name** | Station |
| **Bound Table** | `onto_dim_station` (from `bicycles_gold`) |
| **Primary Key** | `station_key` (BigInt) |

**Properties:**

| Column | Type | Notes |
|--------|------|-------|
| `station_key` | BigInt | **Primary Key** |
| `station_id` | String | TfL station identifier |
| `street_address` | String | |
| `neighbourhood` | String | Area name |
| `latitude` | Double | |
| `longitude` | Double | |
| `total_docks` | BigInt | |
| `station_size` | String | Small/Medium/Large |
| `current_status` | String | |
| `current_utilization_pct` | Double | 0.0-1.0 (multiply x100 for %) |
| `rebalance_priority` | Double | |
| `zone` | String | |
| `loaded_at` | DateTime | ETL timestamp |

---

### 2.2 - Neighbourhood (Dimension)

| Setting | Value |
|---------|-------|
| **Name** | Neighbourhood |
| **Bound Table** | `dim_neighbourhood` |
| **Primary Key** | `neighbourhood_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `neighbourhood_key` | BigInt |
| `neighbourhood_name` | String |
| `station_count` | BigInt |
| `total_capacity` | BigInt |
| `total_bikes_available` | BigInt |
| `total_empty_docks` | BigInt |
| `neighbourhood_utilization_pct` | Double |
| `avg_utilization_pct` | Double |
| `health_score` | Double |
| `capacity_status` | String |
| `empty_stations` | BigInt |
| `full_stations` | BigInt |
| `stations_needing_rebalance` | BigInt |
| `centroid_lat` | Double |
| `centroid_lon` | Double |
| `density_tier` | String |
| `loaded_at` | DateTime |

---

### 2.3 - TimeSlot (Dimension)

| Setting | Value |
|---------|-------|
| **Name** | TimeSlot |
| **Bound Table** | `dim_time` |
| **Primary Key** | `time_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `time_key` | BigInt |
| `hour_of_day` | BigInt |
| `hour_label` | String |
| `time_period` | String |
| `is_rush_hour` | Boolean |
| `am_pm` | String |
| `demand_tier` | String |

---

### 2.4 - CalendarDate (Dimension)

| Setting | Value |
|---------|-------|
| **Name** | CalendarDate |
| **Bound Table** | `dim_date` |
| **Primary Key** | `date_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `date_key` | BigInt |
| `date_value` | DateTime |
| `year` | BigInt |
| `quarter` | BigInt |
| `month_num` | BigInt |
| `month_name` | String |
| `week_of_year` | BigInt |
| `day_of_month` | BigInt |
| `day_of_week` | BigInt |
| `day_name` | String |
| `is_weekend` | Boolean |
| `season` | String |

---

### 2.5 - WeatherObservation (Dimension)

| Setting | Value |
|---------|-------|
| **Name** | WeatherObservation |
| **Bound Table** | `dim_weather` |
| **Primary Key** | `weather_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `weather_key` | BigInt |
| `observation_hour` | DateTime |
| `observation_date` | DateTime |
| `time_key` | BigInt |
| `weather_description` | String |
| `weather_category` | String |
| `weather_severity` | String |
| `has_precipitation` | Boolean |
| `temperature_c` | Double |
| `feels_like_c` | Double |
| `relative_humidity` | Double |
| `wind_speed_kmh` | Double |
| `wind_gust_kmh` | Double |
| `wind_direction_desc` | String |
| `visibility_km` | Double |
| `cloud_cover_pct` | BigInt |
| `pressure_mb` | Double |
| `uv_index` | BigInt |
| `is_daytime` | Boolean |
| `cycling_comfort_index` | Double |
| `icon_code` | BigInt |
| `weather_latitude` | Double |
| `weather_longitude` | Double |
| `location_name` | String |
| `loaded_at` | DateTime |

---

### 2.6 - AvailabilityEvent (Fact)

| Setting | Value |
|---------|-------|
| **Name** | AvailabilityEvent |
| **Bound Table** | `onto_fact_availability` |
| **Primary Key** | `availability_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `availability_key` | BigInt |
| `station_key` | BigInt |
| `event_id` | String |
| `event_timestamp` | DateTime |
| `event_date` | DateTime |
| `date_key` | BigInt |
| `time_key` | BigInt |
| `bikes_available` | BigInt |
| `empty_docks` | BigInt |
| `total_docks` | BigInt |
| `utilization_pct` | Double |
| `event_type` | String |
| `is_critical` | Boolean |
| `availability_band` | String |
| `is_rush_hour` | Boolean |
| `time_period` | String |

---

### 2.7 - HourlyDemand (Fact)

| Setting | Value |
|---------|-------|
| **Name** | HourlyDemand |
| **Bound Table** | `onto_fact_hourly_demand` |
| **Primary Key** | `demand_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `demand_key` | BigInt |
| `neighbourhood_key` | BigInt |
| `event_hour` | DateTime |
| `demand_date` | DateTime |
| `date_key` | BigInt |
| `time_key` | BigInt |
| `event_count` | BigInt |
| `avg_utilization` | Double |
| `min_bikes` | BigInt |
| `max_bikes` | BigInt |
| `avg_bikes` | Double |
| `critical_events` | BigInt |
| `rebalance_triggers` | BigInt |
| `demand_spike_count` | BigInt |
| `active_stations` | BigInt |
| `is_rush_hour` | Boolean |
| `demand_intensity` | String |

---

### 2.8 - RebalancingAssessment (Fact)

| Setting | Value |
|---------|-------|
| **Name** | RebalancingAssessment |
| **Bound Table** | `onto_fact_rebalancing` |
| **Primary Key** | `rebalance_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `rebalance_key` | BigInt |
| `station_key` | BigInt |
| `neighbourhood_key` | BigInt |
| `time_key` | BigInt |
| `weather_key` | BigInt |
| `station_id` | String |
| `neighbourhood` | String |
| `bikes_available` | BigInt |
| `empty_docks` | BigInt |
| `total_docks` | BigInt |
| `utilization_pct` | Double |
| `priority_score` | Double |
| `recommended_action` | String |
| `bikes_to_target` | BigInt |
| `availability_status` | String |
| `station_size` | String |
| `assessed_at` | DateTime |
| `date_key` | BigInt |
| `estimated_rebalance_cost` | Double |

---

### 2.9 - DemandForecast (Fact)

| Setting | Value |
|---------|-------|
| **Name** | DemandForecast |
| **Bound Table** | `onto_forecast_demand` |
| **Primary Key** | `forecast_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `forecast_key` | BigInt |
| `forecast_hour` | DateTime |
| `neighbourhood` | String |
| `neighbourhood_key` | BigInt |
| `time_key` | BigInt |
| `date_key` | BigInt |
| `hour_of_day` | BigInt |
| `day_of_week` | BigInt |
| `is_rush_hour` | Boolean |
| `is_weekend` | Boolean |
| `predicted_demand` | Double |
| `demand_lower_bound` | Double |
| `demand_upper_bound` | Double |
| `demand_tier` | String |
| `pre_position_recommended` | Boolean |
| `forecast_generated_at` | DateTime |
| `model_quality` | String |

---

### 2.10 - WeatherImpact (Fact)

| Setting | Value |
|---------|-------|
| **Name** | WeatherImpact |
| **Bound Table** | `onto_fact_weather_impact` |
| **Primary Key** | `weather_impact_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `weather_impact_key` | BigInt |
| `weather_key` | BigInt |
| `neighbourhood_key` | BigInt |
| `time_key` | BigInt |
| `event_hour` | DateTime |
| `impact_date` | DateTime |
| `date_key` | BigInt |
| `neighbourhood` | String |
| `event_count` | BigInt |
| `weather_adjusted_demand` | Double |
| `weather_demand_impact` | Double |
| `temperature_c` | Double |
| `feels_like_c` | Double |
| `wind_speed_kmh` | Double |
| `has_precipitation` | Boolean |
| `cloud_cover_pct` | BigInt |
| `cycling_comfort_index` | Double |
| `weather_severity` | String |
| `weather_category` | String |
| `weather_description` | String |
| `visibility_km` | Double |
| `impact_category` | String |
| `demand_gap` | Double |
| `loaded_at` | DateTime |

---

### 2.11 - StationSnapshot (Gold - current state)

| Setting | Value |
|---------|-------|
| **Name** | StationSnapshot |
| **Bound Table** | `onto_station_snapshot` |
| **Primary Key** | `station_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `station_key` | BigInt |
| `station_id` | String |
| `street_address` | String |
| `neighbourhood` | String |
| `latitude` | Double |
| `longitude` | Double |
| `total_docks` | BigInt |
| `station_size` | String |
| `zone` | String |
| `bikes_available` | BigInt |
| `empty_docks` | BigInt |
| `utilization_pct` | Double |
| `availability_band` | String |
| `is_critical` | Boolean |
| `last_event_at` | DateTime |
| `last_event_type` | String |
| `rebalance_priority` | Double |
| `operational_status` | String |
| `needs_attention` | Boolean |
| `snapshot_at` | DateTime |

---

### 2.12 - RecentAvailability (Gold - last 24h)

| Setting | Value |
|---------|-------|
| **Name** | RecentAvailability |
| **Bound Table** | `onto_availability_recent` |
| **Primary Key** | `recent_key` (BigInt) |

**Properties:**

| Column | Type |
|--------|------|
| `recent_key` | BigInt |
| `station_key` | BigInt |
| `station_id` | String |
| `street_address` | String |
| `neighbourhood` | String |
| `zone` | String |
| `event_hour` | DateTime |
| `event_date` | DateTime |
| `date_key` | BigInt |
| `time_key` | BigInt |
| `event_count` | BigInt |
| `avg_bikes` | Double |
| `min_bikes` | BigInt |
| `max_bikes` | BigInt |
| `avg_empty_docks` | Double |
| `avg_utilization` | Double |
| `critical_events` | BigInt |
| `utilization_band` | String |
| `loaded_at` | DateTime |

---

## Step 3: Add Relationships

After all 12 entity types are created, add the following 23 relationships.

For each relationship:
1. Click **+ Add relationship**
2. Set the **Source entity** and **Target entity**
3. Name the relationship
4. Configure the **Contextualization** (see Step 4)

### Relationship Reference Table

| # | Relationship Name | Source -> Target | Join Logic |
|---|---|---|---|
| 1 | `located_at` | AvailabilityEvent -> Station | FK: `station_key` on source table |
| 2 | `occurred_during` | AvailabilityEvent -> TimeSlot | FK: `time_key` on source table |
| 3 | `event_on_date` | AvailabilityEvent -> CalendarDate | FK: `date_key` on source table |
| 4 | `measured_in` | HourlyDemand -> Neighbourhood | FK: `neighbourhood_key` on source table |
| 5 | `demand_at_time` | HourlyDemand -> TimeSlot | FK: `time_key` on source table |
| 6 | `demand_on_date` | HourlyDemand -> CalendarDate | FK: `date_key` on source table |
| 7 | `assessed_for` | RebalancingAssessment -> Station | FK: `station_key` on source table |
| 8 | `rebalance_in_zone` | RebalancingAssessment -> Neighbourhood | FK: `neighbourhood_key` on source table |
| 9 | `rebalance_at_time` | RebalancingAssessment -> TimeSlot | FK: `time_key` on source table |
| 10 | `assessed_on_date` | RebalancingAssessment -> CalendarDate | FK: `date_key` on source table |
| 11 | `rebalance_during_weather` | RebalancingAssessment -> WeatherObservation | FK: `weather_key` on source table |
| 12 | `affected_by` | WeatherImpact -> WeatherObservation | FK: `weather_key` on source table |
| 13 | `impacts_zone` | WeatherImpact -> Neighbourhood | FK: `neighbourhood_key` on source table |
| 14 | `impact_at_time` | WeatherImpact -> TimeSlot | FK: `time_key` on source table |
| 15 | `impact_on_date` | WeatherImpact -> CalendarDate | FK: `date_key` on source table |
| 16 | `current_snapshot` | StationSnapshot -> Station | FK: `station_key` on source table |
| 17 | `recent_at_station` | RecentAvailability -> Station | FK: `station_key` on source table |
| 18 | `recent_at_time` | RecentAvailability -> TimeSlot | FK: `time_key` on source table |
| 19 | `recent_on_date` | RecentAvailability -> CalendarDate | FK: `date_key` on source table |
| 20 | `forecast_for_zone` | DemandForecast -> Neighbourhood | FK: `neighbourhood_key` on source table |
| 21 | `forecast_at_time` | DemandForecast -> TimeSlot | FK: `time_key` on source table |
| 22 | `forecast_on_date` | DemandForecast -> CalendarDate | FK: `date_key` on source table |
| 23 | `observed_during` | WeatherObservation -> TimeSlot | FK: `time_key` on source table |

---

## Step 4: Configure Data Bindings and Contextualizations

### Data Bindings (Entity -> Lakehouse Table)

When you add an entity type and select a bound table, Fabric auto-creates the data binding.
Ensure each binding maps **all properties** to their matching columns.

**Key rule:** Every property name must exactly match a column name in the bound table.

### Contextualizations (Relationship -> Join Logic)

For each relationship, the **contextualization** defines how the source and target entities
are joined through the source entity's bound table.

**Pattern for all 23 relationships:**

`Source Table` = Source entity's bound table
`Source Key Binding` = Source entity's PK column mapped to source PK property
`Target Key Binding` = FK column in source table mapped to target entity's PK property

### Complete Contextualization Reference

| Relationship | Source Table | Source PK Column | Target FK Column |
|---|---|---|---|
| `located_at` | `onto_fact_availability` | `availability_key` | `station_key` |
| `occurred_during` | `onto_fact_availability` | `availability_key` | `time_key` |
| `event_on_date` | `onto_fact_availability` | `availability_key` | `date_key` |
| `measured_in` | `onto_fact_hourly_demand` | `demand_key` | `neighbourhood_key` |
| `demand_at_time` | `onto_fact_hourly_demand` | `demand_key` | `time_key` |
| `demand_on_date` | `onto_fact_hourly_demand` | `demand_key` | `date_key` |
| `assessed_for` | `onto_fact_rebalancing` | `rebalance_key` | `station_key` |
| `rebalance_in_zone` | `onto_fact_rebalancing` | `rebalance_key` | `neighbourhood_key` |
| `rebalance_at_time` | `onto_fact_rebalancing` | `rebalance_key` | `time_key` |
| `assessed_on_date` | `onto_fact_rebalancing` | `rebalance_key` | `date_key` |
| `rebalance_during_weather` | `onto_fact_rebalancing` | `rebalance_key` | `weather_key` |
| `affected_by` | `onto_fact_weather_impact` | `weather_impact_key` | `weather_key` |
| `impacts_zone` | `onto_fact_weather_impact` | `weather_impact_key` | `neighbourhood_key` |
| `impact_at_time` | `onto_fact_weather_impact` | `weather_impact_key` | `time_key` |
| `impact_on_date` | `onto_fact_weather_impact` | `weather_impact_key` | `date_key` |
| `current_snapshot` | `onto_station_snapshot` | `station_key` | `station_key` |
| `recent_at_station` | `onto_availability_recent` | `recent_key` | `station_key` |
| `recent_at_time` | `onto_availability_recent` | `recent_key` | `time_key` |
| `recent_on_date` | `onto_availability_recent` | `recent_key` | `date_key` |
| `forecast_for_zone` | `onto_forecast_demand` | `forecast_key` | `neighbourhood_key` |
| `forecast_at_time` | `onto_forecast_demand` | `forecast_key` | `time_key` |
| `forecast_on_date` | `onto_forecast_demand` | `forecast_key` | `date_key` |
| `observed_during` | `dim_weather` | `weather_key` | `time_key` |

---

## Step 5: Refresh the Graph Model

After all entity types, relationships, data bindings, and contextualizations are configured:

1. Save the ontology
2. Fabric **auto-provisions** a graph model (named `Bicycle_Ontology_Model_New_graph`)
3. Open the graph model from the workspace
4. Click **Refresh now** in the Schedule panel
5. Wait for the refresh to complete (typically 2-5 minutes)

> **Important:** The graph refresh CANNOT be triggered via API for ontology-managed graphs
> (`InvalidJobType` error). This single UI click is the only manual step required.

---

## Step 6: Connect the Data Agent

After the graph model is refreshed and has data:

1. Open **Bicycle Fleet Intelligence Agent** (Data Agent)
2. Go to **Data sources** settings
3. Add or verify the ontology data source:
   - Type: **Ontology / Graph**
   - Select: `Bicycle_Ontology_Model_New`
4. Save the agent configuration

The agent can now answer graph-backed questions like entity traversals,
relationship lookups, and neighbourhood-scoped queries.

---

## Verification Checklist

- [ ] 12 entity types created
- [ ] All properties mapped for each entity (check counts match tables above)
- [ ] Primary keys set (each entity has PK on its `_key` column)
- [ ] 23 relationships created with correct source/target directions
- [ ] Data bindings configured for every entity
- [ ] Contextualizations set for every relationship
- [ ] Graph model auto-provisioned and visible in workspace
- [ ] Graph refreshed via UI (status: Query Ready)
- [ ] Agent connected and can answer: "Show stations in Sands End"

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| Entity type has 0 properties | Binding not configured | Re-bind to the correct lakehouse table |
| Relationship shows no data | Contextualization missing | Add contextualization with correct key bindings |
| Graph refresh fails | Tables don't exist yet | Run `PL_BicycleRTI_Medallion` pipeline first |
| Graph shows 0 nodes | Wrong lakehouse ID in binding | Re-select `bicycles_gold` as the data source |
| Agent returns empty results | Graph not refreshed | Click Refresh now in graph model |
| Agent can't find ontology | Data source not added | Add ontology as data source in agent settings |
