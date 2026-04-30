# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Telematics Pipeline Demo
# MAGIC
# MAGIC This notebook runs the bronze, silver, and gold pipeline stages.

# COMMAND ----------

# In Databricks Repos, this may already be on the Python path.
# If needed, adjust sys.path to include the repo root.

import sys
repo_root = "/Workspace/Repos/<your-user-or-team>/realtime_telematics_databricks_pipeline"
if repo_root not in sys.path:
    sys.path.append(repo_root)

# COMMAND ----------

from src.jobs.run_pipeline import main

main()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.telematics_demo.gold_vehicle_activity_by_day
# MAGIC ORDER BY event_date DESC, vehicle_id
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.telematics_demo.gold_driver_behavior_by_day
# MAGIC ORDER BY event_date DESC, driver_id
# MAGIC LIMIT 100;
