# Agent Sample Questions Guide

> Tested questions that work with the **Bicycle Fleet Intelligence Agent** and
> **Ontology Data Agent**. Organized by data source and category to help demo
> presenters and new users get instant results.

---

## How the Agent Routes Questions

The Bicycle Fleet Intelligence Agent has **3 data sources** and automatically
routes your question to the right one:

| Data Source | Best For | Engine |
|-------------|----------|--------|
| **Semantic Model** (Bicycle RTI Analytics) | KPIs, aggregates, DAX measures | DAX queries |
| **Lakehouse** (bicycles_gold) | Detail rows, filtered lookups, JOINs | T-SQL queries |
| **Ontology/Graph** (Bicycle_Ontology_Model_New) | Entity traversals, neighborhood scoping | GQL queries |

**Routing rules:**
- KPIs and measures -> Semantic Model (DAX)
- Detail rows with filters -> Lakehouse (SQL)
- "Show me entities connected to X" -> Graph (GQL)

---

## Category 1: Station Status (Quick Wins)

These query `gold_station_snapshot` (~120 rows) and return fast.

| # | Question | Expected Result |
|---|----------|-----------------|
| 1 | **Which stations currently have the lowest bike availability?** | List of stations sorted by bikes_available ASC |
| 2 | **What is the current status of all stations?** | All ~120 stations with operational_status and utilization |
| 3 | **Which stations need attention right now?** | Stations where needs_attention = true |
| 4 | **How many stations are empty or full right now?** | Count of stations at 0% or 100% utilization |
| 5 | **Show me all critical stations** | Stations where is_critical = true |
| 6 | **What is the fleet utilization summary by station size?** | Avg utilization grouped by Small/Medium/Large |

---

## Category 2: Recent Availability (Last 24 Hours)

These query `gold_availability_recent` or `onto_availability_recent` (~2,880 rows).

| # | Question | Expected Result |
|---|----------|-----------------|
| 7 | **What is the average station utilization during morning rush hour?** | Avg utilization for is_rush_hour = true |
| 8 | **How many critical events happened in the last 24 hours?** | Count of critical_events |
| 9 | **Show hourly utilization trend for the last 24 hours** | Hourly avg_utilization grouped by event_hour |
| 10 | **What happened at station 001297 in the last 24 hours?** | Hourly breakdown for that station |
| 11 | **What is the utilization distribution across all stations?** | Breakdown by utilization_band |

---

## Category 3: Demand Analysis

These query `fact_hourly_demand` / `onto_fact_hourly_demand` (~3,200 rows).

| # | Question | Expected Result |
|---|----------|-----------------|
| 12 | **Which neighbourhoods have the highest demand right now?** | Top neighbourhoods by event_count |
| 13 | **Show me the hourly demand trend for today** | event_count by hour for current date |
| 14 | **Compare rush hour vs off-peak demand across neighbourhoods** | Pivot by is_rush_hour x neighbourhood |
| 15 | **Which neighbourhoods experience the most demand spikes?** | Ranked by demand_spike_count |

---

## Category 4: Rebalancing Operations

These query `fact_rebalancing` / `onto_fact_rebalancing`.

| # | Question | Expected Result |
|---|----------|-----------------|
| 16 | **What stations need rebalancing most urgently?** | Top stations by priority_score DESC |
| 17 | **What is the total rebalancing cost by neighbourhood?** | SUM(estimated_rebalance_cost) grouped by neighbourhood |
| 18 | **Show the breakdown of station availability status** | Count by availability_status (Empty/Critical/Low/Normal/High/Full) |
| 19 | **Which empty or critically low stations need bikes delivered?** | Stations with recommended_action = 'Add Bikes' |

---

## Category 5: Weather Impact

These query `fact_weather_impact` / `onto_fact_weather_impact` (~3,200 rows).

| # | Question | Expected Result |
|---|----------|-----------------|
| 20 | **What is the current weather and cycling comfort?** | Latest weather observation with cycling_comfort_index |
| 21 | **How does weather severity break down over the last week?** | Count by weather_severity (Mild/Moderate/Severe) |
| 22 | **What are the best and worst cycling hours based on comfort index?** | Hours ranked by cycling_comfort_index |
| 23 | **How does rain affect demand compared to sunny weather?** | Avg demand by weather_category |
| 24 | **Show the weather impact distribution by category** | Count by impact_category |
| 25 | **Which weather conditions suppress demand the most?** | Lowest weather_adjusted_demand grouped by weather_description |

---

## Category 6: Demand Forecasting

These query `forecast_demand` / `onto_forecast_demand` (~700 rows).

| # | Question | Expected Result |
|---|----------|-----------------|
| 26 | **What are the demand forecasts for the next 24 hours?** | predicted_demand by forecast_hour |
| 27 | **Which neighbourhoods need pre-positioning of bikes?** | Where pre_position_recommended = true |
| 28 | **Compare predicted vs actual demand for today** | forecast_demand JOIN fact_hourly_demand |
| 29 | **What is the forecast accuracy by neighbourhood?** | model_quality distribution |

---

## Category 7: Neighbourhood Insights

These query `dim_neighbourhood` (19 rows) - very fast.

| # | Question | Expected Result |
|---|----------|-----------------|
| 30 | **List all neighbourhoods with their health scores** | 19 neighbourhoods with health_score |
| 31 | **Which neighbourhoods have the most stations needing rebalance?** | Ranked by stations_needing_rebalance |
| 32 | **Show neighbourhood capacity status** | capacity_status per neighbourhood |

---

## Category 8: Customer Analysis

These query `dim_customers` (56 rows).

| # | Question | Expected Result |
|---|----------|-----------------|
| 33 | **Which premium customers are near Sands End?** | Customers where Neighbourhood = 'Sands End' AND MembershipTier = 'Premium' |
| 34 | **How many customers are in each neighbourhood by membership tier?** | Count by Neighbourhood x MembershipTier |
| 35 | **Which customers opted in for marketing in affected areas?** | MarketingOptIn = true in neighbourhoods with issues |

---

## Category 9: Time and Calendar

These query `dim_time` (24 rows) and `dim_date` (365 rows).

| # | Question | Expected Result |
|---|----------|-----------------|
| 36 | **Weekend vs weekday demand comparison** | Avg demand split by is_weekend |
| 37 | **What is the seasonal demand pattern?** | Demand grouped by season |
| 38 | **Show me demand by time period** | Demand grouped by time_period (Morning/Afternoon/Evening/Night) |

---

## Category 10: Ontology / Graph Questions

These use the **ontology data agent** or the graph data source of the fleet agent.
They traverse entity relationships via GQL queries.

> **Important:** These only work AFTER the graph model has been refreshed
> (see ONTOLOGY_SETUP_GUIDE.md Step 5).

| # | Question | What It Traverses |
|---|----------|-------------------|
| 39 | **Show me all stations in Sands End** | Station entities filtered by neighbourhood |
| 40 | **What availability events are linked to station 001297?** | AvailabilityEvent --located_at--> Station |
| 41 | **Which weather observations affected Sands End?** | WeatherImpact --impacts_zone--> Neighbourhood |
| 42 | **Show rebalancing assessments for stations in Wandsworth** | RebalancingAssessment --assessed_for--> Station (filtered) |
| 43 | **What demand forecasts exist for Chelsea?** | DemandForecast --forecast_for_zone--> Neighbourhood |
| 44 | **Show me station snapshots and their linked stations** | StationSnapshot --current_snapshot--> Station |
| 45 | **What weather was observed during the morning rush?** | WeatherObservation --observed_during--> TimeSlot |

### Graph Question Limitations

The Fabric Graph GQL engine does **not** currently support:
- `COUNT()`, `SUM()`, `AVG()` aggregations
- `ORDER BY` / `TOP N` sorting
- Complex subqueries

For aggregation questions, the agent will route to Lakehouse SQL or Semantic Model DAX instead.

---

## Category 11: DAX Measure Questions (Semantic Model)

These leverage the 47 pre-built DAX measures in `Bicycle RTI Analytics`.

| # | Question | DAX Measure Used |
|---|----------|------------------|
| 46 | **What is the overall fleet utilization?** | [Fleet Utilization %] |
| 47 | **How many stations are in critical status?** | [Critical Stations Count] |
| 48 | **What is the availability SLA percentage?** | [Availability SLA %] |
| 49 | **Show me the total rebalancing cost** | [Total Rebalance Cost] |
| 50 | **What is the current cycling comfort index?** | [Current Comfort Index] |
| 51 | **How many bikes are available across the fleet?** | [Total Bikes Available] |
| 52 | **What is today's predicted demand?** | [Predicted Demand Today] |

### Key Benchmarks

| Metric | Target | What It Means |
|--------|--------|---------------|
| Fleet Utilization | 50-70% | Balanced supply/demand |
| Availability SLA | >95% | Stations rarely empty |
| Rebalance Cost | <$2/bike | Efficient logistics |
| Cycling Comfort | >70 | Good cycling conditions |
| Forecast MAPE | <15% | Accurate predictions |

---

## Demo Flow (Recommended Order)

For a live demo, ask questions in this order for maximum impact:

### Act 1: Current State (2 min)
1. "What is the overall fleet utilization?"
2. "Which stations need attention right now?"
3. "Show me all critical stations"

### Act 2: Why (3 min)
4. "What is the current weather and cycling comfort?"
5. "How does rain affect demand compared to sunny weather?"
6. "Which neighbourhoods have the highest demand?"

### Act 3: What Next (2 min)
7. "What are the demand forecasts for the next 24 hours?"
8. "Which neighbourhoods need pre-positioning of bikes?"
9. "What stations need rebalancing most urgently?"

### Act 4: Graph Traversal (2 min)
10. "Show me all stations in Sands End"
11. "What availability events are linked to station 001297?"
12. "Which weather observations affected Sands End?"

### Act 5: Customer Impact (1 min)
13. "Which premium customers are near Sands End?"
14. "Which customers opted in for marketing in affected areas?"

---

## Performance Tips

| Table | Rows | Tip |
|-------|------|-----|
| `gold_station_snapshot` | ~120 | Query freely - instant results |
| `gold_availability_recent` | ~2,880 | Query freely - fast |
| `dim_neighbourhood` | 19 | Query freely - instant |
| `dim_time` | 24 | Query freely - instant |
| `dim_date` | 365 | Query freely - instant |
| `dim_weather` | ~2,000 | Query freely - fast |
| `dim_customers` | 56 | Query freely - instant |
| `fact_hourly_demand` | ~3,200 | Query freely - fast |
| `fact_weather_impact` | ~3,200 | Query freely - fast |
| `forecast_demand` | ~700 | Query freely - fast |
| `fact_rebalancing` | varies | Add date/time filters for large datasets |
| `fact_availability` | varies | **Always filter by date** - can be very large |

> **Rule:** If asking about `fact_availability` or `fact_rebalancing`,
> always include a time range (e.g., "today", "last 24 hours", "this week").
