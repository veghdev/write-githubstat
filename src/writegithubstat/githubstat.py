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

    @staticmethod
    def _get_auth_header(token: str) -> dict:
        auth_header = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.spiderman-preview+json",
        }
        return auth_header

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def repo(self) -> str:
        return self._repo

    @property
    def header(self) -> dict:
        return self._header


class GithubStatType(ABC):
    def __init__(self, owner: str, repo: str) -> None:
        self._owner = owner
        self._repo = repo

    @property
    @abstractmethod
    def urls(self):
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
    def process_stat(responses):
        pass


class Referrers(GithubStatType):
    @property
    def urls(self):
        return [
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/popular/referrers"
        ]

    @property
    def dimensions(self):
        return ["referrer"]

    @property
    def measures(self):
        return ["count", "uniques"]

    @staticmethod
    def process_stat(responses):
        data = responses[0]
        df = pd.DataFrame(data)
        return df


class Paths(GithubStatType):
    @property
    def urls(self):
        return [
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/popular/paths"
        ]

    @property
    def dimensions(self):
        return ["path", "title"]

    @property
    def measures(self):
        return ["count", "uniques"]

    @staticmethod
    def process_stat(responses):
        data = responses[0]
        df = pd.DataFrame(data)
        return df


class StarsForks(GithubStatType):
    @property
    def urls(self):
        return [f"https://api.github.com/repos/{self._owner}/{self._repo}"]

    @property
    def dimensions(self):
        return []

    @property
    def measures(self):
        return ["stars", "forks"]

    @staticmethod
    def process_stat(responses):
        data = responses[0]
        stars = data["stargazers_count"]
        forks = data["forks_count"]
        df = pd.DataFrame({"stars": [stars], "forks": [forks]})
        return df


class ViewsClones(GithubStatType):
    @property
    def urls(self):
        return [
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/views",
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/clones",
        ]

    @property
    def dimensions(self):
        return []

    @property
    def measures(self):
        return []

    @staticmethod
    def process_stat(responses):
        views = responses[0]["views"][-1]
        clones = responses[1]["clones"][-1]
        df = pd.DataFrame(
            {
                "views_total": [views["count"]],
                "views_unique": [views["uniques"]],
                "clones_total": [clones["count"]],
                "clones_unique": [clones["uniques"]],
            }
        )
        return df


class _GithubStat:
    @staticmethod
    def _get_stat(stat_type: GithubStatType, auth_header: dict) -> pd.DataFrame:
        responses = []
        for url in stat_type.urls:
            response = requests.get(url, headers=auth_header)
            if response.status_code == 200:
                responses.append(response.json())
            else:
                raise requests.HTTPError(
                    f"Request failed with status code {response.status_code}: {response.text}"
                )
        return stat_type.process_stat(responses)


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
