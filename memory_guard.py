import ctypes
import gc
import os
import threading
from contextlib import contextmanager


_CONDITION = threading.Condition()
_ACTIVE_TASK = ""
_PRIORITY_WAITERS = 0


@contextmanager
def memory_intensive_task(label, priority=False):
    """Run one high-memory background unit at a time.

    Planogram parsing uses priority so a long image/catalogue queue cannot keep a
    newly uploaded PDF waiting. The lock is process-local, matching Gunicorn's
    single-worker deployment.
    """
    global _ACTIVE_TASK, _PRIORITY_WAITERS
    label = str(label or "background")[:40]

    with _CONDITION:
        if priority:
            _PRIORITY_WAITERS += 1
            try:
                while _ACTIVE_TASK:
                    _CONDITION.wait()
                _ACTIVE_TASK = label
            finally:
                _PRIORITY_WAITERS -= 1
        else:
            while _ACTIVE_TASK or _PRIORITY_WAITERS:
                _CONDITION.wait()
            _ACTIVE_TASK = label

    try:
        yield
    finally:
        with _CONDITION:
            _ACTIVE_TASK = ""
            _CONDITION.notify_all()


def current_rss_mb():
    """Current resident memory on Render/Linux and local Windows development."""
    try:
        with open("/proc/self/statm", "r", encoding="ascii") as fh:
            resident_pages = int(fh.read().split()[1])
        return round(resident_pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024), 1)
    except (OSError, ValueError, IndexError, AttributeError):
        pass
    if os.name == "nt":
        try:
            from ctypes import wintypes

            class ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            psapi.GetProcessMemoryInfo.argtypes = [
                wintypes.HANDLE, ctypes.POINTER(ProcessMemoryCounters), wintypes.DWORD
            ]
            psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
            handle = kernel32.GetCurrentProcess()
            if psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                return round(counters.WorkingSetSize / (1024 * 1024), 1)
        except (AttributeError, OSError, ValueError):
            pass
    return None


def memory_snapshot():
    with _CONDITION:
        return {
            "rss_mb": current_rss_mb(),
            "active_task": _ACTIVE_TASK or None,
            "planogram_waiters": _PRIORITY_WAITERS,
        }


def release_unused_memory():
    """Collect Python objects and return free glibc arenas to Render when possible."""
    gc.collect()
    if os.name != "posix":
        return
    try:
        malloc_trim = getattr(ctypes.CDLL(None), "malloc_trim", None)
        if malloc_trim is not None:
            malloc_trim(0)
    except (AttributeError, OSError):
        pass
