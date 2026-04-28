# beer-ml

Poetry-managed project for beer recipe data tooling.

## Recipe scraper

Scrapes all public recipes from Brewer's Friend (~322k) by discovering URLs
from their sitemaps and parsing the structured text block on each recipe page.

### Quick start

```bash
cd beer-ml
poetry install
poetry shell

# Scrape all recipes (default: web mode, 3 workers, ~0.5 recipes/s)
python pull_brewersfriend_recipes.py

# Limit to first N recipes
python pull_brewersfriend_recipes.py --max-recipes 500

# Adjust concurrency / delay
python pull_brewersfriend_recipes.py --workers 2 --delay 0.5
```

Output goes to `exports/brewersfriend/recipes.jsonl` — one JSON object per line.

The scraper **resumes automatically**: re-running skips already-scraped recipe IDs.

### Modes

| Flag | Description |
|------|-------------|
| `--source web` | (default) Scrape public recipe pages |
| `--source api` | Use the private API (own recipes only, requires `bf_api_key` in `.env`) |
| `--source sitemap` | Discover IDs via sitemaps, then call `/v1/recipes/{id}` API |

### Output format (web mode)

Each line in `recipes.jsonl` is a JSON object with:

- `id`, `url`, `title`, `brew_method`, `style_name`
- `original_gravity`, `final_gravity`, `abv_standard`, `ibu_tinseth`, `srm_morey`
- `batch_size`, `boil_size`, `boil_time`, `efficiency`
- `fermentables` — list of `{amount, name, bill_pct}`
- `hops` — list of `{amount, name, type, alpha_acid, use, ibu}`
- `other_ingredients` — list of `{amount, name, time, type, use}`
- `yeast` — `{name, starter, form, attenuation_avg, flocculation, ...}`
- `mash_steps`, `water_profile`, `priming`
