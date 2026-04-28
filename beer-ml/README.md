# beer-ml

Poetry-managed project for beer recipe data tooling.

## Recipe export

- Default (auto): `python pull_brewersfriend_recipes.py`
- Force API listing mode: `python pull_brewersfriend_recipes.py --source api`
- Force sitemap mode: `python pull_brewersfriend_recipes.py --source sitemap`
- Provide custom sitemap URLs:
  `python pull_brewersfriend_recipes.py --source sitemap --sitemap-url <url1> --sitemap-url <url2>`
