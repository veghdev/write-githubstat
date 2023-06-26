# write-githubstat
write-githubstat makes it easy to collect, filter and save github statistics to csv files.

[![PyPI version](https://badge.fury.io/py/write-githubstat.svg)](https://badge.fury.io/py/write-githubstat)


# About The Project

write-githubstat makes it easy to collect, filter and save github statistics to csv files.

# Installation

write-githubstat requires `pandas` package.

```sh
pip install write-githubstat
```

# Usage

```python
import logging
from pathlib import Path

from writegithubstat import WriteGithubStat, GithubAuth, Referrers, Paths, StarsForks, ViewsClones


logging.basicConfig(level=logging.INFO)

owner = "owner"
repo = "repo"
token = "token"

auth = GithubAuth(owner, repo, token)
write_githubstat = WriteGithubStat(auth)
for stat_type in (
    Referrers(owner, repo),
    Paths(owner, repo),
    StarsForks(owner, repo),
    ViewsClones(owner, repo, write_githubstat.date),
):
    year = write_githubstat.date[0:4]
    outdir = "stats"
    outfile = (
        f"{year}_githubstat_{stat_type.__class__.__name__.lower()}.csv"
    )
    csv = Path(outdir) / outfile
    write_githubstat.write_stat(stat_type, csv)
```

# License

Copyright © 2023.

Released under the [Apache 2.0 License](https://github.com/veghdev/write-condastat/blob/main/LICENSE).
