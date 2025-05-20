"""
PieData это БД на основе множества файлов.
Каждая таблица это отдельная папка.
А каждая запись это отдельный файл.
"""
from datetime import datetime
from typing import Union, Optional
import asyncio
import websockets
class PieField:
    def __init__(self, initial_value = None, is_required: bool = False):
        self.initial_value = initial_value
        self.is_required = is_required

    def validate(self, value):
        if not self.is_required:    
            return True
        else:
            return True if value is not None else False 

class StringField(PieField):
    def __init__(self, initial_value: str = None, max_length = None, is_required: bool = False):
        super().__init__(initial_value, is_required)
        self.max_length = max_length

    def validate(self, value):
        if super().validate(value):
            return (value is None) or (isinstance(value, str) and self._validate_length(value))
        else:
            return False

    def _validate_length(self, value):
        return (self.max_length is None) or (len(value) <= self.max_length)

class IntegerField(PieField):
    def __init__(self, initial_value: int = None, max_value = None, min_value = None, is_required: bool = False):
        super().__init__(initial_value, is_required)
        self.max_value = max_value
        self.min_value = min_value

    def validate(self, value):
        if super().validate(value):
            return (value is None) or (isinstance(value, int) and self._validate_value(value))
        else:
            return False

    def _validate_value(self, value):
        return (value >= self.min_value if self.min_value is not None else True) and (value <= self.max_value if self.max_value is not None else True)

class FloatField(PieField):
    def __init__(self, initial_value: int = None, max_value = None, min_value = None, is_required: bool = False):
        super().__init__(initial_value, is_required)
        self.max_value = max_value
        self.min_value = min_value

    def validate(self, value):
        if super().validate(value):
            return (value is None) or (isinstance(value, float) and self._validate_value(value))
        else:
            return False

    def _validate_value(self, value):
        return (value >= self.min_value if self.min_value is not None else True) and (value <= self.max_value if self.max_value is not None else True)

class DatetimeField(PieField):
    SUPPORTED_FORMATS = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%d.%m.%Y",
        "%m/%d/%Y",
        "%H:%M:%S",
        "%H:%M",
    ]
    
    def __init__(self, initial_value: Optional[Union[str, datetime]] = None, 
                 is_required: bool = False, 
                 custom_formats: Optional[list[str]] = None):
        super().__init__(initial_value, is_required)
        self.formats = self.SUPPORTED_FORMATS.copy()
        if custom_formats:
            self.formats.extend(custom_formats)
    
    def validate(self, value: Union[str, datetime, None]) -> bool:
        if not super().validate(value):
            return False
        
        if value is None:
            return True
            
        if isinstance(value, datetime):
            return True
            
        if isinstance(value, str):
            for fmt in self.formats:
                try:
                    datetime.strptime(value, fmt)
                    return True
                except ValueError:
                    continue
                    
        return False


class PieModelMeta(type):
    def __new__(self, name, bases, namespace):
        fields = {
            name: field for name, field in namespace.items() if isinstance(field, PieField)
        }
        new_namespace = namespace.copy()
        for name in fields.keys():
            del new_namespace[name]
        new_namespace['_fields'] = fields
        new_namespace['_table_name'] = new_namespace['__qualname__']
        return super().__new__(self, name, bases, new_namespace)

class PieModel(metaclass=PieModelMeta):

    def __init__(self, *args, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        for name, field in self._fields.items():
            if not hasattr(self, name):
                setattr(self, name, field.initial_value)

    def __setattr__(self, key, value):
        if key in self._fields:
            if self._fields[key].validate(value):
                super().__setattr__(key, value)
            else:
                raise AttributeError('Invalid value "{}" for field "{}"'.format(value, key))
        else:
            raise AttributeError('Unknown field "{}"'.format(key))

    def __str__(self):
        new_dictionary = {}
        for name in self._fields.keys():
            new_dictionary[name] = getattr(self, name)
        return str(new_dictionary)
    
    @classmethod
    def _create_table_sql(cls) -> str:
        data = []
        for key, value in cls._fields.items():
            type_name = type(value).__name__.removesuffix("Field")
            data.append(f"{key} {type_name}")
        res = f"({", ".join(data)})"
        return f"create table {cls._table_name} {res}"

    @staticmethod
    def _parse_value(value) -> str:
        if isinstance(value, int | float):
            return str(value)
        return f"'{value}'"

    def _insert_into_sql(self):
        data = self._fields.keys()
        return f"insert into {self._table_name} ({", ".join(data)}) values ({", ".join(self._parse_value(getattr(self, key)) for key in data)})"



class PieDB():
    def __init__(
            self, 
            *tables: PieModel,
            db_dir: str = "database", 
            db_address: str = "localhost", 
            db_port: int = 8765
            ):
        self.tables = tables
        self.root_dir = db_dir
        self.uri = f"ws://{db_address}:{db_port}"
        self.websocket = None
        self.loop = asyncio.get_event_loop()

    async def connect(self):
        if self.websocket is None or self.websocket.closed:
            self.websocket = await websockets.connect(self.uri)

    async def send_command(self, command):
        await self.connect()
        await self.websocket.send(command)
        response = await self.websocket.recv()
        return response

    def command(self, command):
        return self.loop.run_until_complete(self.send_command(command))

    def check_connection(self):
        return self.command("CHECK CONNECTION")

    async def close(self):
        if self.websocket:
            await self.websocket.close()
 
    def __del__(self):
        self.loop.run_until_complete(self.close())



        
