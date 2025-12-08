from datetime import datetime, timedelta
from typing import Sequence
from api_client import CVSE_Client, RPCTimeProtocol, RankingInfoEntryProtocol, RecordingNewEntryProtocol, IndexProtocol, RPCTime, capnp_to_Rank, Rank, asyncMapInBatch
import csv
import capnp
import asyncio

def rpc_time_to_datetime(rpc_time: RPCTimeProtocol) -> datetime:
    return datetime.fromtimestamp(rpc_time.seconds + rpc_time.nanoseconds / 1_000_000_000)

# class RecordingNewEntryProtocol(Protocol[T, T1]):
#     avid: str
#     bvid: str
#     title: str
#     uploader: str
#     upFace: str
#     copyright: int
#     pubdate: RPCTimeProtocol[T1]
#     duration: int
#     page: int
#     cover: str
#     desc: str
#     tags: list[str]
#     is_examined: bool
#     ranks: list[RankProtocol[T1]]
#     is_republish: bool
#     staff_info: str
async def __main():
    current_time = datetime.now()
    # get saturday 0.00 of this week
    end_of_week = current_time.replace(hour=0, minute=0, second=0, microsecond=0) - \
        timedelta(days=current_time.weekday() - 5)
    start_of_week = end_of_week - timedelta(days=7)
    client = await CVSE_Client.create("47.104.152.246", "8663")
    entries: list[IndexProtocol] = await client.getAll(
        True,
        True,
        RPCTime.from_datetime(start_of_week),
        RPCTime.from_datetime(end_of_week)
    )
    meta_info_entries: Sequence[RecordingNewEntryProtocol] = await client.lookupMetaInfo(entries)
    with open("this_week_recordings.csv", "w", newline="", encoding="utf-8-sig") as csvfile:
        fieldnames = [
            "avid", "bvid", "title", "uploader", "upFace", "copyright",
            "pubdate", "duration", "page", "cover", "desc", "tags",
            "is_examined",
            "in_domestic",
            "in_sv",
            "in_utau",
            "is_republish",
            "staff_info"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in meta_info_entries:
            ranks = list(map(capnp_to_Rank, entry.ranks))
            writer.writerow({
                "avid": entry.avid,
                "bvid": entry.bvid,
                "title": entry.title,
                "uploader": entry.uploader,
                "upFace": entry.upFace,
                "copyright": entry.copyright,
                "pubdate": rpc_time_to_datetime(entry.pubdate).strftime("%Y-%m-%d %H:%M:%S"),
                "duration": entry.duration,
                "page": entry.page,
                "cover": entry.cover,
                "desc": entry.desc,
                "tags": ";".join(entry.tags),
                "is_examined": entry.isExamined,
                "in_domestic": Rank.DOMESTIC in ranks,
                "in_sv": Rank.SV in ranks,
                "in_utau": Rank.UTAU in ranks,
                "is_republish": entry.isRepublish,
                "staff_info": entry.staffInfo
            })
async def get_domestic_68():
    client = await CVSE_Client.create("47.104.152.246", "8663")
    start_time = datetime.now()
    await client.reCalculateRankings(
        Rank.DOMESTIC,
        68,
        True,
        False
    )
    print(f"Recalculated rankings in {datetime.now() - start_time}")
    stat = await client.lookupRankingMetaInfo(Rank.DOMESTIC, 68, True)
    entries = []
    rank_step = 10000
    for from_rank in range(1, stat.count + 1, rank_step):
        new_indices: Sequence[IndexProtocol] = await client.getAllRankingInfo(
            Rank.DOMESTIC,
            68,
            True,
            from_rank=from_rank,
            to_rank=from_rank + rank_step
        )
        size = len(new_indices)
        async for new_entries in asyncMapInBatch(
            lambda indices: client.lookupRankingInfo(Rank.DOMESTIC, 68, True, indices),
            new_indices,
            batch_size=4096
        ):
            new_entries = list(new_entries)
            size -= len(new_entries)
            entries.extend(new_entries)
        if size > 0:
            print(f"Warning: {size} entries not fetched")
        print(f"Fetched rankings from {from_rank} to {from_rank + rank_step}")
    print(f"Total entries: {len(entries)}")
    print(f"""
        count: {stat.count}
        totalNew: {stat.totalNew}
        totalView: {stat.totalView}
        totalLike: {stat.totalLike}
        """)
def main():
    asyncio.run(capnp.run(get_domestic_68()))
