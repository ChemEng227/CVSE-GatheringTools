import enum
import capnp
import sys
sys.path.append("CVSE-API")
import CVSE_capnp

class Rank(enum.Enum):
    DOMESTIC = 1
    SV = 2
    UTAU = 3

class DataEntry:
    def __init__(self, 
                 avid: str,
                 bvid: str,
                 ranks: list[Rank] | None,
                 is_republish: bool | None,
                 staff: str | None,
                ):
        self.avid = avid
        self.bvid = bvid
        self.ranks = ranks
        self.is_republish = is_republish
        self.staff = staff

def updateDataEntry(entries: list[DataEntry]) -> None:
    # TODO
    pass

def main():
    print("This is a test function in utils.py")