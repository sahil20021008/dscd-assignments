from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Point(_message.Message):
    __slots__ = ("x", "y")
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    x: float
    y: float
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ...) -> None: ...

class status(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: str
    def __init__(self, state: _Optional[str] = ...) -> None: ...

class Identity(_message.Message):
    __slots__ = ("reducer_id", "mapper_id")
    REDUCER_ID_FIELD_NUMBER: _ClassVar[int]
    MAPPER_ID_FIELD_NUMBER: _ClassVar[int]
    reducer_id: int
    mapper_id: int
    def __init__(self, reducer_id: _Optional[int] = ..., mapper_id: _Optional[int] = ...) -> None: ...

class mapData(_message.Message):
    __slots__ = ("startIdx", "endIdx", "iter_num", "reducers", "mapper_id", "centroids")
    STARTIDX_FIELD_NUMBER: _ClassVar[int]
    ENDIDX_FIELD_NUMBER: _ClassVar[int]
    ITER_NUM_FIELD_NUMBER: _ClassVar[int]
    REDUCERS_FIELD_NUMBER: _ClassVar[int]
    MAPPER_ID_FIELD_NUMBER: _ClassVar[int]
    CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    startIdx: int
    endIdx: int
    iter_num: int
    reducers: int
    mapper_id: int
    centroids: _containers.RepeatedCompositeFieldContainer[Point]
    def __init__(self, startIdx: _Optional[int] = ..., endIdx: _Optional[int] = ..., iter_num: _Optional[int] = ..., reducers: _Optional[int] = ..., mapper_id: _Optional[int] = ..., centroids: _Optional[_Iterable[_Union[Point, _Mapping]]] = ...) -> None: ...

class reduceData(_message.Message):
    __slots__ = ("iter_num", "my_reducer_id", "work_reducer_id", "mappers")
    ITER_NUM_FIELD_NUMBER: _ClassVar[int]
    MY_REDUCER_ID_FIELD_NUMBER: _ClassVar[int]
    WORK_REDUCER_ID_FIELD_NUMBER: _ClassVar[int]
    MAPPERS_FIELD_NUMBER: _ClassVar[int]
    iter_num: int
    my_reducer_id: int
    work_reducer_id: int
    mappers: int
    def __init__(self, iter_num: _Optional[int] = ..., my_reducer_id: _Optional[int] = ..., work_reducer_id: _Optional[int] = ..., mappers: _Optional[int] = ...) -> None: ...

class Pair(_message.Message):
    __slots__ = ("centroid_id", "point")
    CENTROID_ID_FIELD_NUMBER: _ClassVar[int]
    POINT_FIELD_NUMBER: _ClassVar[int]
    centroid_id: float
    point: Point
    def __init__(self, centroid_id: _Optional[float] = ..., point: _Optional[_Union[Point, _Mapping]] = ...) -> None: ...

class Pairs(_message.Message):
    __slots__ = ("pairs",)
    PAIRS_FIELD_NUMBER: _ClassVar[int]
    pairs: _containers.RepeatedCompositeFieldContainer[Pair]
    def __init__(self, pairs: _Optional[_Iterable[_Union[Pair, _Mapping]]] = ...) -> None: ...

class ReduceResp(_message.Message):
    __slots__ = ("pairs", "state")
    PAIRS_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    pairs: _containers.RepeatedCompositeFieldContainer[Pair]
    state: str
    def __init__(self, pairs: _Optional[_Iterable[_Union[Pair, _Mapping]]] = ..., state: _Optional[str] = ...) -> None: ...
