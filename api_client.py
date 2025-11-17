import enum
import os
import sys
from functools import singledispatch
from typing import Any, Iterable, TypedDict, TypeVar

import capnp

current_script_path = os.path.abspath(__file__)
subdir = "CVSE-API"
sys.path.append(os.path.join(os.path.dirname(current_script_path), subdir))
import CVSE_capnp


class Rank(enum.Enum):
    DOMESTIC = 1
    SV = 2
    UTAU = 3


class ModifyEntry(TypedDict):
    avid: str
    bvid: str
    ranks: list[Rank] | None
    is_republish: bool | None
    staff: str | None


class AddressingNewEntry(TypedDict):
    avid: str
    bvid: str
    title: str
    uploader: str
    up_face: str
    copyright: int
    pubdate: str
    duration: int
    page: int
    cover: str
    desc: str
    tags: list[str]


class AddressingDataEntry(TypedDict):
    avid: str
    bvid: str
    view: int
    favorite: int
    coin: int
    like: int
    danmaku: int
    reply: int
    share: int
    date: dict[str, int]  # e.g., {"year": 2023, "month": 10, "day": 5}


T = TypeVar("T")


def build_list_to_capnp(l: Iterable[T], builder) -> None:
    capnp_list = builder
    for i, item in enumerate(l):
        capnp_list[i] = item
    return capnp_list


@singledispatch
def to_capnp(obj: Any) -> None:
    raise NotImplementedError(f"Unsupported type: {type(obj)}")


@to_capnp.register(Rank)
def _(obj: Rank):
    rank = CVSE_capnp.Rank.new_message()
    rank = rank.init("value", 1)[0]
    match obj:
        case Rank.DOMESTIC:
            rank.type = "domestic"
        case Rank.SV:
            rank.type = "sv"
        case Rank.UTAU:
            rank.type = "utau"
    return rank


@to_capnp.register(ModifyEntry)
def _(obj: ModifyEntry):
    entry = CVSE_capnp.ModifyEntry.new_message()
    entry.avid = int(obj["avid"])
    entry.bvid = obj["bvid"]
    if obj["ranks"] is not None:
        ranks = map(to_capnp, obj["ranks"])
        build_list_to_capnp(ranks, entry.init("ranks", len(obj["ranks"])))
    if obj["is_republish"] is not None:
        entry.isRepublish = obj["is_republish"]
    if obj["staff"] is not None:
        entry.staff = obj["staff"]
    return entry


@to_capnp.register(AddressingNewEntry)
def _(obj: AddressingNewEntry):
    entry = CVSE_capnp.AddressingNewEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.title = obj["title"]
    entry.uploader = obj["uploader"]
    entry.upFace = obj["up_face"]
    entry.copyright = obj["copyright"]
    entry.pubdate = obj["pubdate"]
    entry.duration = obj["duration"]
    entry.page = obj["page"]
    entry.cover = obj["cover"]
    entry.desc = obj["desc"]
    build_list_to_capnp(obj["tags"], entry.init("tags", len(obj["tags"])))
    return entry


@to_capnp.register(AddressingDataEntry)
def _(obj: AddressingDataEntry):
    entry = CVSE_capnp.AddressingDataEntry.new_message()
    entry.avid = obj["avid"]
    entry.bvid = obj["bvid"]
    entry.view = obj["view"]
    entry.favorite = obj["favorite"]
    entry.coin = obj["coin"]
    entry.like = obj["like"]
    entry.danmaku = obj["danmaku"]
    entry.reply = obj["reply"]
    entry.share = obj["share"]
    date = entry.date
    date.year = obj["date"]["year"]
    date.month = obj["date"]["month"]
    date.day = obj["date"]["day"]
    return entry


class CVSE_Client:
    def __init__(self, connection):
        self.connection = connection
        self.client = capnp.TwoPartyClient(connection)
        self.cvse = self.client.bootstrap().cast_as(CVSE_capnp.CVSE)

    async def updateModifyEntry(self, entries: list[ModifyEntry]) -> None:
        size = len(entries)
        entries_capnp = map(to_capnp, entries)
        request = self.cvse.updateModifyEntry_request()
        build_list_to_capnp(entries_capnp, request.init("entries", size))
        await request.send()

    async def updateNewEntry(self, entries: list[AddressingNewEntry]) -> None:
        size = len(entries)
        entries_capnp = map(to_capnp, entries)
        request = self.cvse.updateNewEntry_request()
        build_list_to_capnp(entries_capnp, request.init("entries", size))
        await request.send()

    async def updateAddressingDataEntry(
        self, entries: list[AddressingDataEntry]
    ) -> None:
        size = len(entries)
        entries_capnp = map(to_capnp, entries)
        request = self.cvse.updateAddressingDataEntry_request()
        build_list_to_capnp(entries_capnp, request.init("entries", size))
        await request.send()


def main():
    print("This is a test function in utils.py")
