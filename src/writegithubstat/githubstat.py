from abc import ABC, abstractmethod
import os
from pathlib import Path
import logging
import requests
from datetime import date

import pandas as pd


class GithubAuth:
    def __init__(self, owner: str, repo: str, token: str) -> None:
        self._owner = owner
        self._repo = repo
        self._header = GithubAuth._get_auth_header(token)
        self._repo_id = GithubAuth._get_repo_id(self._owner, self._repo, self._header)

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
                f"Request failed with status code {response.status_code}: {response.text}"
            )

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def repo(self) -> str:
        return self._repo

    @property
    def repo_id(self) -> str:
        return self._repo_id

    @property
    def header(self) -> dict:
        return self._header


class GithubStatType(ABC):
    def __init__(self, auth: GithubAuth) -> None:
        self._auth = auth

    @property
    @abstractmethod
    def url(self):
        pass

    @property
    @abstractmethod
    def dimensions(self):
        pass

    @property
    @abstractmethod
    def measures(self):
        pass

    @staticmethod
    def process_stat(data):
        pass


class Referrers(GithubStatType):
    @property
    def url(self):
        return f"https://api.github.com/repositories/{self._auth.repo_id}/traffic/popular/referrers"

    @property
    def dimensions(self):
        return ["referrer"]

    @property
    def measures(self):
        return ["count", "uniques"]

    @staticmethod
    def process_stat(data):
        df = pd.DataFrame(data)
        return df


class Paths(GithubStatType):
    @property
    def url(self):
        return f"https://api.github.com/repositories/{self._auth.repo_id}/traffic/popular/paths"

    @property
    def dimensions(self):
        return ["path", "title"]

    @property
    def measures(self):
        return ["count", "uniques"]

    @staticmethod
    def process_stat(data):
        df = pd.DataFrame(data)
        return df


class Stars(GithubStatType):
    @property
    def url(self):
        return f"https://api.github.com/repos/{self._auth.owner}/{self._auth.repo}"

    @property
    def dimensions(self):
        return []

    @property
    def measures(self):
        return ["stars_cumulative"]

    @staticmethod
    def process_stat(data):
        stars = data["stargazers_count"]
        df = pd.DataFrame({"stars_cumulative": [stars]})
        return df


class _GithubStat:
    @staticmethod
    def _get_stat(stat_type: GithubStatType, auth_header: dict) -> pd.DataFrame:
        response = requests.get(stat_type.url, headers=auth_header)
        if response.status_code == 200:
            return stat_type.process_stat(response.json())
        else:
            raise requests.HTTPError(
                f"Request failed with status code {response.status_code}: {response.text}"
            )


class WriteGithubStat:
    def __init__(self, auth: GithubAuth) -> None:
        self._date = date.today().strftime("%Y-%m-%d")
        self._auth = auth

    @property
    def date(self) -> str:
        return self._date

    def write_stat(self, stat_type: GithubStatType, csv: Path) -> None:
        os.makedirs(csv.parent, exist_ok=True)
        stats = self._get_stats(stat_type)
        logging.info(stats)
        stored_stats = self._get_stored_stats(csv)
        merged_stats = self._merge_stats(stored_stats, stats)
        merged_stats.to_csv(csv, index=False)

    def _get_stats(self, stat_type: GithubStatType) -> pd.DataFrame:
        stat = _GithubStat._get_stat(stat_type, self._auth.header)
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
        df.insert(1, "owner", self._auth.owner)
        df.insert(2, "repo", self._auth.repo)
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
                    & (stored_stats["owner"] == self._auth.owner)
                    & (stored_stats["repo"] == self._auth.repo)
                ].index
            )
            stats = pd.concat([stored_stats, stats])
        return stats
