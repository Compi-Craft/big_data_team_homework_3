CATALOG   = "airbnb_lab3"
BRONZE_DB = "airbnb_bronze"
SILVER_DB = "airbnb_silver"
GOLD_DB   = "airbnb_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_DB}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_DB}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_DB}")

def fq(db: str, table: str) -> str:
    return f"{CATALOG}.{db}.{table}"

VOLUME_ROOT = "/Volumes/airbnb_lab3/default/airbnb"

CITY_SUFFIX = {
    "la":  "",
    "nyc": "-2",
}

def build_path(dataset: str, city: str) -> str:
    suffix = CITY_SUFFIX[city]
    return f"{VOLUME_ROOT}/{dataset}{suffix}.csv*"

CITIES = ["la", "nyc"]
