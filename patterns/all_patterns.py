from enum import Enum

class Patterns(Enum):
    CUP_HANDLE = "cup and handle"
    TD_DIFFERENTIAL_ENTRY = "td differential entry"
    TD_DIFFERENTIAL_EXIT = "td differential exit"
    TD_REVERSE_DIFFERENTIAL_ENTRY = "td reverse differential entry"
    TD_REVERSE_DIFFERENTIAL_EXIT = "td reverse differential exit"
    TD_ANTI_DIFFERENTIAL_ENTRY = "td anti differential entry"
    TD_ANTI_DIFFERENTIAL_EXIT = "td anti differential exit"
    VCP = "vcp"
    FLAT_BASE = "flat base"