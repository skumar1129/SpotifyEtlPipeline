-- This is the final fact table, partitioned by day.
-- It will store the 1.2M+ daily records.
-- It's configured as 'incremental' in dbt_project.yml

{{
  config(
    materialized='incremental',
    partition_by={
      "field": "played_at_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["user_id", "artist_id"]
  )
}}

select
    listen_id,
    user_id,
    track_id,
    artist_id,
    album_id,
    played_at_ts,
    played_at_day
    
from {{ ref('stg_spotify__recently_played') }}

{% if is_incremental() %}

  -- This filter ensures we only overwrite the partition for the current run
  where played_at_day = date("{{ var('run_date', modules.datetime.date.today().isoformat()) }}")

{% endif %}
