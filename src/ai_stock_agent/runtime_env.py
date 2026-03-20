from __future__ import annotations

import os
import sys


def read_env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    if sys.platform != "win32":
        return None
    try:
        import winreg
    except ImportError:
        return None
    registry_paths = [
        (winreg.HKEY_CURRENT_USER, r"Environment"),
        (winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
    ]
    for root, subkey in registry_paths:
        try:
            with winreg.OpenKey(root, subkey) as key:
                value, _ = winreg.QueryValueEx(key, name)
        except OSError:
            continue
        if value:
            return str(value)
    return None
