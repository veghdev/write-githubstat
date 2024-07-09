from abc import ABC, abstractmethod
import os
from pathlib import Path
import logging
from datetime import date, timedelta
from typing import Dict, Any, List, Union

import requests
import pandas as pd


class GithubAuth:
    def __init__(self, owner: str, repo: str, token: str) -> None:
        self._owner = owner
        self._repo = repo
        self._header = self._get_auth_header(token)

    @staticmethod
    def _get_auth_header(token: str) -> Dict[str, str]:
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
    def header(self) -> Dict[str, str]:
        return self._header


class GithubStatType(ABC):
    def __init__(self, owner: str, repo: str) -> None:
        self._owner = owner
        self._repo = repo

    @property
    @abstractmethod
    def urls(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def dimensions(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def measures(self) -> List[str]:
        pass

    @abstractmethod
    def process_stat(self, responses: List[Dict[str, Any]]) -> pd.DataFrame:
        pass


class Referrers(GithubStatType):
    @property
    def urls(self) -> List[str]:
        return [
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/popular/referrers"
        ]

    @property
    def dimensions(self) -> List[str]:
        return ["referrer"]

    @property
    def measures(self) -> List[str]:
        return ["count", "uniques"]

    def process_stat(self, responses: List[Dict[str, Any]]) -> pd.DataFrame:
        data = responses[0]
        df = pd.DataFrame(data)
        return df


class Paths(GithubStatType):
    @property
    def urls(self) -> List[str]:
        return [
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/popular/paths"
        ]

    @property
    def dimensions(self) -> List[str]:
        return ["path"]

    @property
    def measures(self) -> List[str]:
        return ["count", "uniques"]

    def process_stat(self, responses: List[Dict[str, Any]]) -> pd.DataFrame:
        data = responses[0]
        df = pd.DataFrame(data)
        if "title" in df.columns:
            df = df.drop("title", axis=1)
        return df


class StarsForks(GithubStatType):
    @property
    def urls(self) -> List[str]:
        return [f"https://api.github.com/repos/{self._owner}/{self._repo}"]

    @property
    def dimensions(self) -> List[str]:
        return []

    @property
    def measures(self) -> List[str]:
        return ["stars", "forks"]

    def process_stat(self, responses: List[Dict[str, Any]]) -> pd.DataFrame:
        data = responses[0]
        stars = data["stargazers_count"]
        forks = data["forks_count"]
        df = pd.DataFrame({"stars": [stars], "forks": [forks]})
        return df


class ViewsClones(GithubStatType):
    def __init__(self, owner: str, repo: str, date: str) -> None:
        super().__init__(owner, repo)
        self._date = date

    @property
    def urls(self) -> List[str]:
        return [
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/views",
            f"https://api.github.com/repos/{self._owner}/{self._repo}/traffic/clones",
        ]

    @property
    def dimensions(self) -> List[str]:
        return []

    @property
    def measures(self) -> List[str]:
        return ["views_total", "views_unique", "clones_total", "clones_unique"]

    def process_stat(self, responses: List[Dict[str, Any]]) -> pd.DataFrame:
        views = self._get_actual_stat(responses[0], "views")
        clones = self._get_actual_stat(responses[1], "clones")
        df = pd.DataFrame(
            {
                "views_total": [views["count"]],
                "views_unique": [views["uniques"]],
                "clones_total": [clones["count"]],
                "clones_unique": [clones["uniques"]],
            }
        )
        return df

    def _get_actual_stat(
        self, data: Dict[str, Any], name: str
    ) -> Dict[str, Union[int, str]]:
        try:
            for stat in data[name]:
                if stat["timestamp"].startswith(self._date):
                    return stat
            raise ValueError(
                f"The views data for the date {self._date} is not available."
            )
        except (KeyError, IndexError, ValueError):
            return {"count": 0, "uniques": 0}


class GithubStatAPI:
    @staticmethod
    def get_stat(
        stat_type: GithubStatType, auth_header: Dict[str, str]
    ) -> pd.DataFrame:
        responses = []
        for url in stat_type.urls:
            response = requests.get(url, headers=auth_header)
            response.raise_for_status()
            responses.append(response.json())
        return stat_type.process_stat(responses)


class WriteGithubStat:
    def __init__(self, auth: GithubAuth) -> None:
        self._date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        self._auth = auth

    @property
    def date(self) -> str:
        return self._date

    def write_stat(self, stat_type: GithubStatType, csv: Path) -> None:
        os.makedirs(csv.parent, exist_ok=True)
        stats = self._get_stats(stat_type)
        WriteGithubStat._log_df(stats)
        stored_stats = self._get_stored_stats(csv)
        merged_stats = self._merge_stats(stored_stats, stats)
        merged_stats.to_csv(csv, index=False)

    @staticmethod
    def _log_df(df) -> None:
        with pd.option_context('display.max_columns', None,
                               'display.max_rows', None,
                               'display.width', None):
            logging.info(df)

    def _get_stats(self, stat_type: GithubStatType) -> pd.DataFrame:
        stat = GithubStatAPI.get_stat(stat_type, self._auth.header)
        if stat.empty:
            empty = {col: "-" for col in stat_type.dimensions} | {
                col: 0 for col in stat_type.measures
            }
            stat = pd.DataFrame([empty])
        stat = self._insert_metadata(stat)
        return stat

    def _insert_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df.insert(0, "date", self._date)
        df.insert(1, "owner", self._auth.owner)
        df.insert(2, "repo", self._auth.repo)
        return df

    def _get_stored_stats(self, path: Union[str, Path]) -> pd.DataFrame:
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
