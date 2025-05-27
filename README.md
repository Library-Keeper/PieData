**PieData** is an ORM-wrapped file database that supports SQL-like commands via a WebSocket server. Each table is stored as a folder, each record as an XML file. The ORM layer allows you to work with Python models, and the server handles client commands via WebSocket.

---
It's not working properly in linux system

---

## Installation

Install via pip:

```bash
pip install piedata
```

---

## Quickstart

### Define models (`models.py`)

```python
from PieData import IntegerField, StringField, DatetimeField, FloatField, PieModel

class User(PieModel):
    name = StringField(max_length=255)
    age = IntegerField(min_value=0, max_value=100)
```

### Synchronous usage (`main.py`)

```python
from PieData import PieData
from models import User

db = PieData()

db.create_table(User)

user = User(name="Alice", age=30)
db.insert(user)

users = db.select(User, as_model=True)

# Update and delete
updated = db.update(User, {"age": 31}, "name = 'Alice'")
deleted = db.delete(User, "age = 31")
```

### Asynchronous usage (`async_main.py`)

```python
import asyncio
from PieData import PieData
from models import User

async def main():
    async with PieData() as db:
        await db.create_table_async(User)
        await db.insert_async(User(name="Bob", age=25))
        users = await db.select_async(User, as_model=True)

if __name__ == '__main__':
    asyncio.run(main())
```

---

## Command-Line Interface (`piedata`)

After installing you can manage the PieData server with the `piedata` command. The PID file is written to:

* **Windows**: `%APPDATA%\PieDataServer\piedataserver.pid`
* **Linux/macOS**: `~/.PieDataServer/piedataserver.pid`

Available subcommands:

```bash
piedata start         # Start the PieData WebSocket server in the background
piedata stop          # Stop the server by PID
piedata check         # Check PID if server running
piedata restart       # Restart the server (stop + start)
piedata autostart     # Enable OS autostart for the server
piedata noautostart   # Disable OS autostart for the server
```

* **start**: Launches the server in the background.
* **stop**: Reads the PID from the file and terminates the process.
* **restart**: Performs `stop` followed by `start`.
* **autostart** / **noautostart**:

  * On Windows, modifies the registry key `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`.
  * On Linux/macOS, creates or removes the desktop file `~/.config/autostart/piedataserver.desktop`.

Example:

```bash
# Start the server and enable autostart
piedata start
piedata autostart
```

