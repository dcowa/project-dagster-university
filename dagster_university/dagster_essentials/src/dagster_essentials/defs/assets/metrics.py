import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd

import duckdb
import os

from dagster_essentials.defs.assets import constants

# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    query = """
        SELECT
            z.zone
                ,z.borough
                ,z.geometry
                ,count(*) as num_trips

    FROM trips t

    left join zones z 
    on t.pickup_zone_id = z.zone_id

    where borough = 'Manhattan' AND geometry IS NOT NULL

    GROUP BY zone, borough, geometry
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim(-74.05, -73.90)  # Adjust longitude range
    ax.set_ylim(40.70, 40.82)  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)