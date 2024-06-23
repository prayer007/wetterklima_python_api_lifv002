from pydantic.dataclasses import dataclass
from typing import List, Literal, Optional

DatasetType = Literal['station']
DatasetMode = Literal['historical']
DatasetResponseFormats = Literal['geojson', 'netcdf', 'csv']
DatasetFrequency = Literal['1H' ,'1D', '10T', '1M', '1MS', '1Y', '1YS']
DatasetFrequency_v2 = Literal['PT1H' ,'P1D', 'PT10T', 'PT10M', 'P1M', '1MS', 'P1Y', '1YS']
StationType = Literal['INDIVIDUAL', 'COMBINED']

@dataclass
class EndpointData:
    type: DatasetType
    mode: DatasetMode
    response_formats: List[DatasetResponseFormats]
    url: str

@dataclass
class BaseParameter:
    name: str
    long_name: str
    unit: str

@dataclass
class Parameter(BaseParameter):
    desc: str

@dataclass
class Parameter_v2(BaseParameter):
    description: str

@dataclass
class BaseStation:
    type: StationType
    group_id: Optional[str]
    name: str
    state: Optional[str]
    lat: float
    lon: float
    altitude: float
    valid_from: str
    valid_to: str
    has_sunshine: Optional[bool]
    has_global_radiation: Optional[bool]
    is_active: bool

@dataclass
class Station(BaseStation):
    id: str

@dataclass
class Station_v2(BaseStation):
    id: int
    group_id: Optional[int]

@dataclass
class BaseMetadata:
    title: str
    frequency: DatasetFrequency
    type: DatasetType
    mode: DatasetMode
    response_formats: List[str]
    start_time: str
    end_time: str

@dataclass
class Metadata(BaseMetadata):
    parameters: List[Parameter]
    stations: List[Station]

@dataclass
class Metadata_v2(BaseMetadata):
    frequency: DatasetFrequency_v2
    parameters: List[Parameter_v2]
    stations: List[Station_v2]