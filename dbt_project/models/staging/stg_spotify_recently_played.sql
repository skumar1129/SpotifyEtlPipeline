-- This model reads from the external table created by Airflow
-- It cleans and casts types before building the final fact table.

with source_data as (

    select *
    from {{ source('spotify_external', 'stg_external_spotify_listens') }}
    
    -- This WHERE clause is for incremental processing
    -- It ensures we only re-process the partition for the current run date
    where played_at_day = date("{{ var('run_date', modules.datetime.date.today().isoformat()) }}")

)

select
    -- Generate a surrogate key
    {{ dbt_utils.generate_surrogate_key(['track_id', 'played_at_ts']) }} as listen_id,
    
    -- (In a real model, you would get user_id from the session context)
    'default_user' as user_id, 
    
    cast(track_id as string) as track_id,
    cast(artist_id as string) as artist_id,
    cast(album_id as string) as album_id,
    
    cast(played_at_ts as timestamp) as played_at_ts,
    cast(played_at_day as date) as played_at_day,
    
    cast(track_name as string) as track_name,
    cast(artist_name as string) as artist_name,
    cast(album_name as string) as album_name
    
from source_data

-- We assume the Spark job already handled major deduplication
-- but we can add a final check here if needed.
