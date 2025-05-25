import os
import sys
import signal
import subprocess
import platform
from pathlib import Path

# Путь к PID-файлу
if platform.system() == "Windows":
    APP_DIR = Path(os.getenv("APPDATA")) / "PieDataServer"
    STARTUP_REG_KEY = r"Software\\Microsoft\\Windows\\CurrentVersion\\Run"
    PID_FILE = APP_DIR / "piedataserver.pid"
    AUTOSTART_NAME = "PieDataServer"
else:
    APP_DIR = Path.home() / ".PieDataServer"
    PID_FILE = APP_DIR / "piedataserver.pid"
    DESKTOP_AUTOSTART = Path.home() / ".config/autostart/piedataserver.desktop"
SERVER_SCRIPT = Path(__file__)


def main():
    if len(sys.argv) < 2:
        print("Usage: piedataserver start|stop|restart|autostart|noautostart")
        sys.exit(1)
    cmd = sys.argv[1].lower()
    if cmd == 'start':
        start_server()
    elif cmd == 'stop':
        stop_server()
    elif cmd == 'restart':
        restart_server()
    elif cmd == 'autostart':
        enable_autostart()
    elif cmd == 'noautostart':
        disable_autostart()
    elif cmd == "check":
        check_server()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)

def check_server():
    if not PID_FILE.exists():
        print("PID file not found.")
        return

    pid = int(PID_FILE.read_text().strip())
    print(f"Server working with PID {pid}")

def start_server():
    APP_DIR.mkdir(parents=True, exist_ok=True)
    if PID_FILE.exists():
        print("Server already running (pid file exists).")
        return

    python_exe = Path(sys.executable)
    if platform.system() == "Windows":
        python_exe = python_exe.parent / "pythonw.exe"

    cmd = [str(python_exe), str(SERVER_SCRIPT.parent / "server.py")]

    if platform.system() == 'Windows':
        DETACHED_PROCESS = 0x00000008
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
            close_fds=True
        )
    else:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True
        )

    pid = proc.pid
    PID_FILE.write_text(str(pid))
    print(f"Server started with PID {pid}")


def stop_server():
    if not PID_FILE.exists():
        print("PID file not found. Is the server running?")
        return

    pid = int(PID_FILE.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        print("Server stopped")
    except Exception as e:
        print(f"Error stopping server: {e}")
        sys.exit(1)
    finally:
        if PID_FILE.exists():
            PID_FILE.unlink()


def restart_server():
    stop_server()
    start_server()


def enable_autostart():
    """Добавляет сервер в автозапуск"""
    if platform.system() == "Windows":
        import winreg
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, STARTUP_REG_KEY, 0, winreg.KEY_SET_VALUE)
        cmd = f'"{Path(sys.executable).parent / "pythonw.exe"}" "{SERVER_SCRIPT}" restart'
        print(cmd)
        winreg.SetValueEx(key, AUTOSTART_NAME, 0, winreg.REG_SZ, cmd)
        winreg.CloseKey(key)
        print("Autostart enabled (registry key set)")
    else:
        autostart_dir = DESKTOP_AUTOSTART.parent
        autostart_dir.mkdir(parents=True, exist_ok=True)
        desktop_entry = f"""
[Desktop Entry]
Type=Application
Exec={sys.executable} {SERVER_SCRIPT} restart
Hidden=false
NoDisplay=false
X-GNOME-Autostart-enabled=true
Name=PieDataServer
Comment=Auto-start PieDataServer
"""
        DESKTOP_AUTOSTART.write_text(desktop_entry.strip())
        print(f"Autostart file created: {DESKTOP_AUTOSTART}")


def disable_autostart():
    """Удаляет сервер из автозапуска"""
    if platform.system() == "Windows":
        import winreg
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, STARTUP_REG_KEY, 0, winreg.KEY_SET_VALUE)
            winreg.DeleteValue(key, AUTOSTART_NAME)
            winreg.CloseKey(key)
            print("Autostart disabled (registry key removed)")
        except FileNotFoundError:
            print("Autostart key not found.")
    else:
        if DESKTOP_AUTOSTART.exists():
            DESKTOP_AUTOSTART.unlink()
            print(f"Autostart file removed: {DESKTOP_AUTOSTART}")
        else:
            print("Autostart file not found.")

if __name__ == '__main__':
    main()
