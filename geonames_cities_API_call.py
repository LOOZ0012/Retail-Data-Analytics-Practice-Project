import requests

EXPORT = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/geonames-all-cities-with-a-population-1000/exports/csv"

params = {
    "select": "geoname_id,name,country_code,population",
    "where": "population > 1000",
    "order_by": "name ASC",
    "delimiter": ",",      # <-- use commas for proper CSV delimitation
    "with_bom": "true"     # <-- for Excel-friendliness
}

out_path = "geonames_cities_population.csv"
r = requests.get(EXPORT, params=params, timeout=120)
r.raise_for_status()
with open(out_path, "wb") as f:
    f.write(r.content)

print(f"Saved -> {out_path}")      # <-- to be imported into SQL for JOIN
