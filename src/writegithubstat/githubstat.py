from abc import ABC, abstractmethod
import os
import logging
import requests
from datetime import date

import pandas as pd


class _GithubAuth:
    def __init__(self, owner: str, repo: str, token: str) -> None:
        self._header = _GithubAuth._get_auth_header(token)
        self._repo_id = _GithubAuth._get_repo_id(owner, repo, self._header)

    @staticmethod
    def _get_auth_header(token: str) -> dict:
        auth_header = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.spiderman-preview+json",
        }
        return auth_header

    @staticmethod
    def _get_repo_id(owner: str, repo: str, auth_header: str) -> str:
        url = f"https://api.github.com/repos/{owner}/{repo}"
        response = requests.get(url, headers=auth_header)
        if response.status_code == 200:
            repository = response.json()
            return repository["id"]
        else:
            raise requests.HTTPError(
                f"Request failed with status code {response.status_code}"
            )

    @property
    def header(self) -> dict:
        return self._header

    @property
    def repo_id(self) -> str:
        return self._repo_id


class _GithubStat:
    @staticmethod
    def _dict_to_dataframe(data) -> pd.DataFrame:
        df = pd.DataFrame(data)
        return df

    @staticmethod
    def _get_stat(auth: _GithubAuth, path: str) -> pd.DataFrame:
        url = f"https://api.github.com/repositories/{auth.repo_id}/{path}"
        response = requests.get(url, headers=auth.header)
        if response.status_code == 200:
            raw_data = response.json()
            data = _GithubStat._dict_to_dataframe(raw_data)
            return data
        else:
            raise requests.HTTPError(
                f"Request failed with status code {response.status_code}"
            )


class _GithubStatType(ABC):
    @property
    @abstractmethod
    def path(self):
        pass

    @property
    @abstractmethod
    def dimensions(self):
        pass

    @property
    @abstractmethod
    def measures(self):
        pass

    @property
    @abstractmethod
    def name(self):
        pass


class Referrers(_GithubStatType):
    @property
    def path(self):
        return "traffic/popular/referrers"

    @property
    def dimensions(self):
        return ["referrer"]

    @property
    def measures(self):
        return ["count", "uniques"]

    @property
    def name(self):
        return "referrers"


class Paths(_GithubStatType):
    @property
    def path(self):
        return "traffic/popular/paths"

    @property
    def dimensions(self):
        return ["path", "title"]

    @property
    def measures(self):
        return ["count", "uniques"]

    @property
    def name(self):
        return "paths"


class WriteGithubStat:
    def __init__(self, owner: str, repo: str, token: str) -> None:
        self._date = date.today().strftime("%Y-%m-%d")
        self._owner = owner
        self._repo = repo
        self._auth = _GithubAuth(self._owner, self._repo, token)

    def write_stats(self, outdir: str, prefix: str) -> None:
        os.makedirs(outdir, exist_ok=True)
        for stat_type in Referrers(), Paths():
            self._write_stat(stat_type, outdir, prefix)

    def _write_stat(self, stat_type: _GithubStatType, outdir: str, prefix: str) -> None:
        year = self._date[0:4]
        csv = f"{outdir}/{year}_{prefix}_githubstat_{stat_type.name}.csv"
        stats = self._get_stats(stat_type)
        logging.info(stats)
        stored_stats = self._get_stored_stats(csv)
        merged_stats = self._merge_stats(stored_stats, stats)
        merged_stats.to_csv(csv, index=False)

    def _get_stats(self, stat_type: _GithubStatType) -> pd.DataFrame:
        stat = _GithubStat._get_stat(self._auth, stat_type.path)
        if stat.empty:
            empty = {
                **{col: "-" for col in stat_type.dimensions},
                **{col: 0 for col in stat_type.measures},
            }
            stat = pd.DataFrame([empty])
        stat = self._insert_metadata(stat)
        return stat

    def _insert_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df.insert(0, "date", self._date)
        df.insert(1, "owner", self._owner)
        df.insert(2, "repo", self._repo)
        return df

    def _get_stored_stats(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(path)
            return df
        except FileNotFoundError:
            return pd.DataFrame()

    def _merge_stats(
        self, stored_stats: pd.DataFrame, stats: pd.DataFrame
    ) -> pd.DataFrame:
        if not stored_stats.empty:
            stored_stats = stored_stats.drop(
                stored_stats[
                    (stored_stats["date"] == self._date)
                    & (stored_stats["owner"] == self._owner)
                    & (stored_stats["repo"] == self._repo)
                ].index
            )
            stats = pd.concat([stored_stats, stats])
        return stats
