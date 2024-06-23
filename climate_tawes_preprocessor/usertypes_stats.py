from pydantic.dataclasses import dataclass
from typing import List, Literal, Optional, Dict, Union

Period = Literal['h', 'd', 'm', 'y']

@dataclass
class TopBottomStats:
    stations: List[int]

@dataclass
class StatsBase:
    timestamp: int

@dataclass
class CurrentStats(StatsBase):
    top10lbottom10: TopBottomStats

@dataclass
class PeriodStats(StatsBase):
    top10lbottom10: TopBottomStats


@dataclass
class TawesStationClimateStats:
    stats: Dict[Period, Union[StatsBase, CurrentStats, PeriodStats]]
