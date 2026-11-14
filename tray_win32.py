from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Callable

logger = logging.getLogger("TwitchMultiMiner.tray")

try:
    import win32api
    import win32con
    import win32gui

    HAS_WIN32 = True
except ImportError:  # pragma: no cover - non-Windows or missing pywin32
    win32api = None
    win32con = None
    win32gui = None
    HAS_WIN32 = False


class Win32TrayController:
    """Native Windows system tray icon and context menu powered by pywin32."""

    _WM_TRAY = (win32con.WM_USER + 20) if HAS_WIN32 else 0x0414
    _ID_SHOW = 1001
    _ID_START_ALL = 1002
    _ID_STOP_ALL = 1003
    _ID_EXIT = 1004

    def __init__(
        self,
        *,
        app_name: str,
        icon_path: Path,
        on_show: Callable[[], None],
        on_start_all: Callable[[], None],
        on_stop_all: Callable[[], None],
        on_exit: Callable[[], None],
    ) -> None:
        self.app_name = app_name
        self.icon_path = Path(icon_path)
        self.on_show = on_show
        self.on_start_all = on_start_all
        self.on_stop_all = on_stop_all
        self.on_exit = on_exit

        self._thread: threading.Thread | None = None
        self._hwnd: int | None = None
        self._hicon: int | None = None
        self._tooltip = app_name[:120]
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._lock = threading.Lock()
        self._taskbar_created_msg = 0
        self._class_name = f"TwitchMultiMinerTrayClass_{id(self)}"

    @property
    def available(self) -> bool:
        return HAS_WIN32

    def start(self) -> bool:
        if not HAS_WIN32:
            logger.warning("pywin32 не установлен; системный трей отключен")
            return False
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return True
            self._ready.clear()
            self._stopped.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="Win32TrayThread",
                daemon=True,
            )
            self._thread.start()
        return self._ready.wait(timeout=3.0) and self._hwnd is not None

    def stop(self) -> None:
        if not HAS_WIN32:
            return
        hwnd = self._hwnd
        if hwnd:
            try:
                win32api.PostMessage(hwnd, win32con.WM_CLOSE, 0, 0)
            except Exception:
                pass
        thread = self._thread
        if thread and thread.is_alive() and threading.current_thread() is not thread:
            thread.join(timeout=2.5)
        self._thread = None

    def update_tooltip(self, text: str) -> None:
        if not HAS_WIN32:
            return
        clean = " ".join(text.split())[:120] or self.app_name
        self._tooltip = clean
        hwnd = self._hwnd
        hicon = self._hicon
        if hwnd and hicon:
            try:
                win32gui.Shell_NotifyIcon(
                    win32gui.NIM_MODIFY,
                    (
                        hwnd,
                        0,
                        win32gui.NIF_ICON | win32gui.NIF_MESSAGE | win32gui.NIF_TIP,
                        self._WM_TRAY,
                        hicon,
                        self._tooltip,
                    ),
                )
            except Exception:
                pass

    def _run_loop(self) -> None:
        hinst = win32api.GetModuleHandle(None)
        self._taskbar_created_msg = win32gui.RegisterWindowMessage("TaskbarCreated")

        wndclass = win32gui.WNDCLASS()
        wndclass.hInstance = hinst
        wndclass.lpszClassName = self._class_name
        wndclass.lpfnWndProc = self._wnd_proc

        class_atom = None
        try:
            class_atom = win32gui.RegisterClass(wndclass)
            self._hwnd = win32gui.CreateWindow(
                class_atom,
                self._class_name,
                0,
                0,
                0,
                win32con.CW_USEDEFAULT,
                win32con.CW_USEDEFAULT,
                0,
                0,
                hinst,
                None,
            )
            win32gui.UpdateWindow(self._hwnd)
            self._hicon = self._load_icon()
            self._add_notify_icon()
            self._ready.set()
            win32gui.PumpMessages()
        except Exception as exc:
            logger.debug("Ошибка в потоке трея: %s", exc)
            self._ready.set()
        finally:
            self._cleanup(class_atom, hinst)
            self._stopped.set()

    def _load_icon(self) -> int:
        if self.icon_path.exists():
            try:
                flags = win32con.LR_LOADFROMFILE | win32con.LR_DEFAULTSIZE
                return win32gui.LoadImage(
                    0,
                    str(self.icon_path),
                    win32con.IMAGE_ICON,
                    0,
                    0,
                    flags,
                )
            except Exception as exc:
                logger.debug("Не удалось загрузить иконку трея %s: %s", self.icon_path, exc)
        return win32gui.LoadIcon(0, win32con.IDI_APPLICATION)

    def _add_notify_icon(self) -> None:
        if not self._hwnd or not self._hicon:
            return
        nid = (
            self._hwnd,
            0,
            win32gui.NIF_ICON | win32gui.NIF_MESSAGE | win32gui.NIF_TIP,
            self._WM_TRAY,
            self._hicon,
            self._tooltip,
        )
        try:
            win32gui.Shell_NotifyIcon(win32gui.NIM_ADD, nid)
        except Exception:
            try:
                win32gui.Shell_NotifyIcon(win32gui.NIM_MODIFY, nid)
            except Exception as exc:
                logger.debug("Не удалось добавить иконку в трей: %s", exc)

    def _remove_notify_icon(self) -> None:
        if self._hwnd:
            try:
                win32gui.Shell_NotifyIcon(win32gui.NIM_DELETE, (self._hwnd, 0))
            except Exception:
                pass

    def _cleanup(self, class_atom: int | None, hinst: int) -> None:
        self._remove_notify_icon()
        if self._hicon:
            try:
                win32gui.DestroyIcon(self._hicon)
            except Exception:
                pass
            self._hicon = None
        if self._hwnd:
            try:
                win32gui.DestroyWindow(self._hwnd)
            except Exception:
                pass
            self._hwnd = None
        if class_atom is not None:
            try:
                win32gui.UnregisterClass(self._class_name, hinst)
            except Exception:
                pass

    def _wnd_proc(self, hwnd: int, msg: int, wparam: int, lparam: int) -> int:
        if msg == self._taskbar_created_msg and self._taskbar_created_msg != 0:
            self._add_notify_icon()
            return 0
        if msg == self._WM_TRAY:
            if lparam in (win32con.WM_LBUTTONUP, win32con.WM_LBUTTONDBLCLK):
                self._safe_invoke(self.on_show)
            elif lparam in (win32con.WM_RBUTTONUP, win32con.WM_CONTEXTMENU):
                self._show_context_menu(hwnd)
            return 0
        if msg == win32con.WM_COMMAND:
            cmd_id = win32api.LOWORD(wparam)
            if cmd_id == self._ID_SHOW:
                self._safe_invoke(self.on_show)
            elif cmd_id == self._ID_START_ALL:
                self._safe_invoke(self.on_start_all)
            elif cmd_id == self._ID_STOP_ALL:
                self._safe_invoke(self.on_stop_all)
            elif cmd_id == self._ID_EXIT:
                self._safe_invoke(self.on_exit)
            return 0
        if msg == win32con.WM_CLOSE:
            self._remove_notify_icon()
            win32gui.DestroyWindow(hwnd)
            return 0
        if msg == win32con.WM_DESTROY:
            self._remove_notify_icon()
            win32gui.PostQuitMessage(0)
            return 0
        return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)

    def _show_context_menu(self, hwnd: int) -> None:
        menu = win32gui.CreatePopupMenu()
        try:
            win32gui.AppendMenu(menu, win32con.MF_STRING, self._ID_SHOW, "Открыть окно")
            win32gui.SetMenuDefaultItem(menu, self._ID_SHOW, False)
            win32gui.AppendMenu(menu, win32con.MF_SEPARATOR, 0, "")
            win32gui.AppendMenu(menu, win32con.MF_STRING, self._ID_START_ALL, "Запустить все аккаунты")
            win32gui.AppendMenu(menu, win32con.MF_STRING, self._ID_STOP_ALL, "Остановить все")
            win32gui.AppendMenu(menu, win32con.MF_SEPARATOR, 0, "")
            win32gui.AppendMenu(menu, win32con.MF_STRING, self._ID_EXIT, "Выход")

            pos = win32gui.GetCursorPos()
            try:
                win32gui.SetForegroundWindow(hwnd)
            except Exception:
                pass
            win32gui.TrackPopupMenu(
                menu,
                win32con.TPM_LEFTALIGN | win32con.TPM_BOTTOMALIGN | win32con.TPM_RIGHTBUTTON,
                pos[0],
                pos[1],
                0,
                hwnd,
                None,
            )
            win32api.PostMessage(hwnd, win32con.WM_NULL, 0, 0)
        finally:
            win32gui.DestroyMenu(menu)

    @staticmethod
    def _safe_invoke(callback: Callable[[], None]) -> None:
        try:
            callback()
        except Exception as exc:
            logger.debug("Ошибка вызова колбэка трея: %s", exc)
