import asyncio
import enum
import os
import sys
from datetime import datetime
from datetime import timedelta
from typing import Any, AsyncGenerator, Protocol, Iterable, NoReturn, TypedDict, TypeVar

import capnp

current_script_path = os.path.abspath(__file__)
subdir = "CVSE-API"
sys.path.append(os.path.join(os.path.dirname(current_script_path), subdir))
import CVSE_capnp


T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")

class RankProtocol(Protocol[T]):
    value: str

class Rank(enum.Enum):
    DOMESTIC = 1
    SV = 2
    UTAU = 3



class RPCTime:
    def __init__(self, seconds: int, nanoseconds: int) -> None:
        self.seconds = seconds
        self.nanoseconds = nanoseconds

    @staticmethod
    def from_datetime(dt: datetime) -> "RPCTime":
        return RPCTime(int(dt.timestamp()), dt.microsecond * 1000)
    @staticmethod
    def minValue() -> "RPCTime":
        return RPCTime(0, 0)

    def to_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.seconds + self.nanoseconds / 1e9)

class RPCTimeProtocol(Protocol[T]):
    seconds: int
    nanoseconds: int

class ModifyEntry(TypedDict):
    avid: str
    bvid: str
    ranks: list[Rank] | None
    is_republish: bool | None
    staff: str | None
    is_examined: bool | None

class ModifyEntryProtocol(Protocol[T]):
    avid: str
    bvid: str
    ranks: list[Rank] | None
    is_republish: bool | None
    staff: str | None
    is_examined: bool | None

class RecordingNewEntry(TypedDict):
    avid: str
    bvid: str
    title: str
    uploader: str
    up_face: str
    copyright: int
    pubdate: RPCTime
    duration: int
    page: int
    cover: str
    desc: str
    tags: list[str]

class RecordingNewEntryProtocol(Protocol[T]):
    avid: str
    bvid: str
    title: str
    uploader: str
    up_face: str
    copyright: int
    pubdate: RPCTimeProtocol[T1]
    duration: int
    page: int
    cover: str


class RecordingDataEntry(TypedDict):
    avid: str
    bvid: str
    view: int
    favorite: int
    coin: int
    like: int
    danmaku: int
    reply: int
    share: int
    date: RPCTime

class RecordingDataEntryProtocol(Protocol[T]):
    avid: str
    bvid: str
    view: int
    favorite: int
    coin: int
    like: int
    danmaku: int
    reply: int
    share: int
    date: RPCTimeProtocol[T1]


class Index(TypedDict):
    avid: str
    bvid: str

class IndexProtocol(Protocol[T]):
    avid: str
    bvid: str



def build_list_to_capnp(l: Iterable[T], builder) -> None:
    capnp_list = builder
    for i, item in enumerate(l):
        capnp_list[i] = item
    return capnp_list


def RPCTime_to_capnp(obj: RPCTime) -> RPCTimeProtocol[T]:
    time = CVSE_capnp.Cvse.Time.new_message()
    time.seconds = obj.seconds
    time.nanoseconds = obj.nanoseconds
    return time


def capnp_to_RPCTime(obj: RPCTimeProtocol[T]) -> RPCTime:
    return RPCTime(obj.seconds, obj.nanoseconds)


def datetime_to_capnp(obj: datetime) -> RPCTimeProtocol[T]:
    return RPCTime_to_capnp(RPCTime.from_datetime(obj))


def Rank_to_capnp(obj: Rank) -> RankProtocol[T]:
    rank = CVSE_capnp.Cvse.Rank.new_message()
    match obj:
        case Rank.DOMESTIC:
            rank.value = "domestic"
        case Rank.SV:
            rank.value = "sv"
        case Rank.UTAU:
            rank.value = "utau"
    return rank

def capnp_to_Rank(obj: RankProtocol[T]) -> Rank:
    match obj.value:
        case "domestic":
            return Rank.DOMESTIC
        case "sv":
            return Rank.SV
        case "utau":
            return Rank.UTAU
    raise ValueError(f"Unknown rank value: {obj.value}")


def ModifyEntry_to_capnp(obj: ModifyEntry) -> ModifyEntryProtocol[T]:
    entry = CVSE_capnp.Cvse.ModifyEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.hasRanks = obj["ranks"] is not None
    entry.hasStaffInfo = obj["staff"] is not None
    entry.hasIsExamined = obj["is_examined"] is not None
    if obj["ranks"] is not None:
        ranks = map(Rank_to_capnp, obj["ranks"])
        build_list_to_capnp(ranks, entry.init("ranks", len(obj["ranks"])))
    else:
        entry.init("ranks", 0)
    if obj["is_republish"] is not None:
        entry.isRepublish = obj["is_republish"]
    else:
        entry.isRepublish = False
    if obj["staff"] is not None:
        entry.staffInfo = obj["staff"]
    else:
        entry.staffInfo = ""
    if obj["is_examined"] is not None:
        entry.isExamined = obj["is_examined"]
    else:
        entry.isExamined = False
    return entry


def RecordingNewEntry_to_capnp(
    obj: RecordingNewEntry,
) -> RecordingDataEntryProtocol[T]:
    entry = CVSE_capnp.Cvse.RecordingNewEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.title = obj["title"]
    entry.uploader = obj["uploader"]
    entry.upFace = obj["up_face"]
    entry.copyright = obj["copyright"]
    entry.pubdate = RPCTime_to_capnp(obj["pubdate"])
    entry.duration = obj["duration"]
    entry.page = obj["page"]
    entry.cover = obj["cover"]
    entry.desc = obj["desc"]
    build_list_to_capnp(obj["tags"], entry.init("tags", len(obj["tags"])))
    return entry


def RecordingDataEntry_to_capnp(
    obj: RecordingDataEntry,
) -> "CVSE_capnp.Cvse.RecordingDataEntry":
    entry = CVSE_capnp.Cvse.RecordingDataEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.view = obj["view"]
    entry.favorite = obj["favorite"]
    entry.coin = obj["coin"]
    entry.like = obj["like"]
    entry.danmaku = obj["danmaku"]
    entry.reply = obj["reply"]
    entry.share = obj["share"]
    entry.date = RPCTime_to_capnp(obj["date"])
    return entry


def Index_to_capnp(obj: Index) -> IndexProtocol[T]:
    index = CVSE_capnp.Cvse.Index.new_message()
    index.avid = obj["avid"]
    index.bvid = obj["bvid"]
    return index


class CVSE_Client:
    def __init__(self, connection, client, cvse):
        self.connection = connection
        self.client = client
        self.cvse = cvse

    @staticmethod
    async def create(host, port) -> "CVSE_Client":
        connection = await capnp.AsyncIoStream.create_connection(host=host, port=port)
        client = capnp.TwoPartyClient(connection)
        cvse = client.bootstrap().cast_as(CVSE_capnp.Cvse)
        self = CVSE_Client(connection, client, cvse)
        return self

    # 所有函数的参数和返回值都使用序列化格式
    # （也就是类型标注中的 xxxxxProtocol）
    # 内存效率比 Python 原生格式更高
    # 具体 field 参考 Protocol 定义即可

    async def updateModifyEntry(
        self, entries: list[ModifyEntryProtocol[T]]
    ) -> None:
        size = len(entries)
        request = self.cvse.updateModifyEntry_request()
        build_list_to_capnp(entries, request.init("entries", size))
        await request.send()

    async def updateNewEntry(
        self, entries: list[RecordingDataEntryProtocol[T]]
    ) -> None:
        size = len(entries)
        request = self.cvse.updateNewEntry_request()
        build_list_to_capnp(entries, request.init("entries", size))
        await request.send()

    async def updateRecordingDataEntry(
        self, entries: list["CVSE_capnp.Cvse.RecordingDataEntry"]
    ) -> None:
        size = len(entries)
        request = self.cvse.updateRecordingDataEntry_request()
        build_list_to_capnp(entries, request.init("entries", size))
        await request.send()

    async def getAll(
        self, 
        get_unexamined: bool, get_unincluded: bool,
        from_date: RPCTime,
        to_date: RPCTime,
    ) -> list[IndexProtocol[T]]:
        """
        获取 pubdate 处于 [from_date, to_date) 的项的索引
        若 get_unexamined 为 True，则返回值包含未经过收录的索引
        若 get_unincluded 为 True，则返回值包含未收录在任何刊的索引
        """
        request = self.cvse.getAll_request()
        request.get_unexamined = get_unexamined
        request.get_unincluded = get_unincluded
        request.from_date = RPCTime_to_capnp(from_date)
        request.to_date = RPCTime_to_capnp(to_date)
        response = await request.send()
        return response.indices

    async def getAllInBatch(
        self,
        get_unexamined: bool, get_unincluded: bool,
        duration: timedelta,
        unbatch_at: datetime = datetime.strptime("2009-01-01", "%Y-%m-%d")
    ) -> AsyncGenerator[list[IndexProtocol[T]], Any, NoReturn]:
        """
        获取所有索引，分批获取，每批的时间间隔为 duration。
        迭代 end_time < unbatch_at 时，剩下的索引合并成一批。
        返回值为一个异步生成器，生成每一批的索引列表。
        """
        end_time = datetime.now()
        start_time = end_time - duration
        while end_time >= unbatch_at:
            indices = await self.getAll(
                get_unexamined, get_unincluded,
                RPCTime.from_datetime(start_time),
                RPCTime.from_datetime(end_time)
            )
            yield indices
            end_time = start_time
            start_time -= duration
        last_batch_indices = await self.getAll(
            get_unexamined, get_unincluded,
            RPCTime.minValue(),
            RPCTime.from_datetime(end_time)
        )
        yield last_batch_indices
        raise StopAsyncIteration

    async def lookupMetaInfo(
        self,
        indices: list[IndexProtocol[T]],
    ) -> list[RecordingDataEntryProtocol[T]]:
        request = self.cvse.lookupMetaInfo_request()
        build_list_to_capnp(indices, request.init("indices", len(indices)))
        response = await request.send()
        return response.entries

    async def lookupDataInfo(
        self,
        indices: list[IndexProtocol[T]],
        from_date: RPCTime,
        to_date: RPCTime,
    ) -> list[list["CVSE_capnp.Cvse.RecordingDataEntry"]]:
        request = self.cvse.lookupDataInfo_request()
        build_list_to_capnp(indices, request.init("indices", len(indices)))
        request.from_date = RPCTime_to_capnp(from_date)
        request.to_date = RPCTime_to_capnp(to_date)
        response = await request.send()
        return response.entries

    async def lookupOneDataInfo(
        self,
        indices: list[CVSE_capnp.Cvse.Index],
        from_date: RPCTime,
        to_date: RPCTime,
    ) -> list["CVSE_capnp.Cvse.RecordingDataEntry"]:
        request = self.cvse.lookupOneDataInfo_request()
        build_list_to_capnp(indices, request.init("indices", len(indices)))
        request.from_date = RPCTime_to_capnp(from_date)
        request.to_date = RPCTime_to_capnp(to_date)
        response = await request.send()
        return response.entries


async def __test() -> None:
    client = await CVSE_Client.create("47.104.152.246", "8663")
    test_new_entries: list[RecordingNewEntry] = [
        {
            "avid": "av1",
            "bvid": "bv1",
            "title": "title1",
            "desc": "description1",
            "tags": ["tag1", "tag2"],
            "cover": "cover1",
            "duration": 100,
            "uploader": "uploader1",
            "up_face": "face1",
            "copyright": 1,
            "pubdate": RPCTime.from_datetime(datetime.now()),
            "page": 1,
        },
        {
            "avid": "av2",
            "bvid": "bv2",
            "title": "title2",
            "desc": "description2",
            "tags": ["tag3", "tag4"],
            "cover": "cover2",
            "duration": 200,
            "uploader": "uploader2",
            "up_face": "face2",
            "copyright": 2,
            "pubdate": RPCTime.from_datetime(datetime.now()),
            "page": 2,
        },
    ]
    test_data_entries: list[RecordingDataEntry] = [
        {
            "avid": "av1",
            "bvid": "bv1",
            "view": 100,
            "danmaku": 50,
            "reply": 20,
            "favorite": 30,
            "coin": 40,
            "share": 50,
            "like": 60,
            "date": RPCTime.from_datetime(datetime.now()),
        },
        {
            "avid": "av2",
            "bvid": "bv2",
            "view": 200,
            "danmaku": 100,
            "reply": 40,
            "favorite": 60,
            "coin": 80,
            "share": 100,
            "like": 120,
            "date": RPCTime.from_datetime(datetime.now()),
        },
    ]
    test_modify_entries: list[ModifyEntry] = [
        {
            "avid": "av1",
            "bvid": "bv1",
            "is_republish": True,
            "ranks": [],
            "staff": "sss",
            "is_examined": True,
        },
        {
            "avid": "av2",
            "bvid": "bv2",
            "is_republish": False,
            "ranks": [Rank.DOMESTIC],
            "staff": None,
            "is_examined": False,
        },
    ]
    test_new_entries1 = list(map(RecordingNewEntry_to_capnp, test_new_entries))
    # await client.updateNewEntry(test_new_entries1)
    test_data_entries1 = list(map(RecordingDataEntry_to_capnp, test_data_entries))
    test_data_entries2 = test_data_entries1 * 100
    test_modify_entries1 = list(map(ModifyEntry_to_capnp, test_modify_entries))
    task1 = asyncio.create_task(client.updateRecordingDataEntry(test_data_entries2))
    task2 = asyncio.create_task(client.updateModifyEntry(test_modify_entries1))
    await asyncio.gather(task1, task2)
    all_info: list[IndexProtocol[T]] = []
    async for batch in client.getAllInBatch(True, True):
        all_info.extend(batch)
    all_meta_info = await client.lookupMetaInfo(all_info)
    for index in all_info:
        print(f"avid: {index.avid}, bvid: {index.bvid}")
    for meta_info in all_meta_info:
        print(
            f"avid: {meta_info.avid}, bvid: {meta_info.bvid}, title: {meta_info.title}"
        )
    all_data = await client.lookupDataInfo(
        all_info, RPCTime.minValue(), RPCTime.from_datetime(datetime.now())
    )
    for datas in all_data:
        for data in datas:
            print(
                f"{data.bvid}: {data.view} at {capnp_to_RPCTime(data.date).to_datetime()}"
            )

    return


def test():
    asyncio.run(capnp.run(__test()))
