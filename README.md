**PieDB** is an ORM-wrapped file database that supports SQL-like commands via WebSocket server. Each table is stored as a folder, each record as an XML file. The ORM layer allows you to work with Python models, and the server handles client commands via WebSocket.

# Usage example

### models.py
```python
from PieData import IntegerField, StringField, DatetimeField, FloatField, PieModel

class User(PieModel):
    name = StringField(max_length=255)
    age = IntegerField(min_value=0, max_value=100)
```
### main.py
```python
from PieData import PieDB
from models import User

db = PieDB()

db.create_table(User)

user = User(name="Alice", age=30)
db.insert(user)

users = db.select(User, as_model=True)

db.update(User, {"age": 31}, "name = 'Alice'")

db.delete(User, "age = 31")
```

# Async usage

```python
async def main():
    async with PieDB() as db:
        await db.create_table_async(User)
        await db.insert_async(User(name="Bob", age=25))
        users = await db.select_async(User, as_model=True)
```