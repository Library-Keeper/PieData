from datetime import datetime
import json
from typing import Type, Dict, Any, TypeVar
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
    def __init__(self, initial_value: float = None, max_value = None, min_value = None, is_required: bool = False):
        super().__init__(initial_value, is_required)
        self.max_value = max_value
        self.min_value = min_value
        
    def validate(self, value):
        if super().validate(value):
            return (value is None) or (isinstance(value, (float, int)) and self._validate_value(value))
        else:
            return False
            
    def _validate_value(self, value):
        return (value >= self.min_value if self.min_value is not None else True) and (value <= self.max_value if self.max_value is not None else True)

class DatetimeField(PieField):
    def __init__(self, initial_value: datetime = None, is_required: bool = False):
        super().__init__(initial_value, is_required)
        
    def validate(self, value):
        if super().validate(value):
            return (value is None) or isinstance(value, datetime)
        else:
            return False

class PieModelMeta(type):
    def __new__(mcs, name, bases, attrs):
        cls = super().__new__(mcs, name, bases, attrs)
        
        cls._fields = {}
        
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, PieField):
                cls._fields[attr_name] = attr_value
                
        return cls

T = TypeVar('T', bound='PieModel')

class PieModel(metaclass=PieModelMeta):
    def __init__(self, **kwargs):
        for field_name, field in self.__class__._fields.items():
            setattr(self, field_name, field.initial_value)
            
        for key, value in kwargs.items():
            if key in self.__class__._fields:
                field = self.__class__._fields[key]
                if field.validate(value):
                    setattr(self, key, value)
                else:
                    raise ValueError(f"Недопустимое значение для поля {key}: {value}")
            else:
                raise AttributeError(f"Неизвестное поле {key} для модели {self.__class__.__name__}")
    
    def to_dict(self) -> Dict[str, Any]: 
        result = {}
        for field_name in self.__class__._fields:
            value = getattr(self, field_name)
            result[field_name] = value
        return result
    
    @classmethod
    def get_table_name(cls) -> str:
        return cls.__name__
    
    @classmethod
    def get_field_types(cls) -> Dict[str, str]:       
        field_types = {}
        for field_name, field in cls._fields.items():
            if isinstance(field, IntegerField):
                field_types[field_name] = "Integer"
            elif isinstance(field, StringField):
                field_types[field_name] = "String"
            elif isinstance(field, FloatField):
                field_types[field_name] = "Float"
            elif isinstance(field, DatetimeField):
                field_types[field_name] = "Datetime"
        return field_types
    
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:  
        kwargs = {}
        for field_name, field_type in cls._fields.items():
            if field_name in data:
                value = data[field_name]
                if isinstance(field_type, DatetimeField) and isinstance(value, str):
                    try:
                        value = datetime.fromisoformat(value)
                    except ValueError:
                        pass
                kwargs[field_name] = value
        return cls(**kwargs)

class PieData:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self._connection = None

        models = PieModel.__subclasses__()
        for model in models:
            if not self.check_table(model):
                self.create_table(model)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    async def __aenter__(self):    
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):  
        await self.close_async()
        
    def _format_value(self, value):    
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, datetime):
            return f"'{value.isoformat()}'"
        else:
            return str(value)
    
    async def _connect(self):  
        if self._connection is None or self._connection.state != websockets.protocol.State.CLOSED:
            self._connection = await websockets.connect(f"ws://{self.host}:{self.port}")
        return self._connection
    
    async def _execute_async(self, query):
        ws = await self._connect()
        await ws.send(query)
        response = await ws.recv()
        return response
    
    def _execute(self, query):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._execute_async(query))
        finally:
            loop.close()
    
    async def close_async(self): 
        if self._connection and not self._connection.state != websockets.protocol.State.CLOSED:
            await self._connection.close()
            self._connection = None
    
    def close(self):   
        if self._connection and not self._connection.state != websockets.protocol.State.CLOSED:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.close_async())
            finally:
                loop.close()
    
    def execute(self, query):  
        return self._execute(query)
    
    def create_table(self, model_class):    
        table_name = model_class.get_table_name()
        field_types = model_class.get_field_types()
        
        columns_str = ", ".join([f"{field_name} {field_type}" for field_name, field_type in field_types.items()])
        query = f"CREATE TABLE {table_name} ({columns_str})"
        return self._execute(query)
    
    def check_connection(self):      
        return self._execute("CHECK CONNECTION")
    
    def check_table(self, model_class_or_name):
        if isinstance(model_class_or_name, str):
            table_name = model_class_or_name
        else:
            table_name = model_class_or_name.get_table_name()

        query = f"CHECK TABLE {table_name}"
        response = self._execute(query)
        return json.loads(response)[0]
    
    def drop_table(self, model_class_or_name, if_exists=True):   
        if isinstance(model_class_or_name, str):
            table_name = model_class_or_name
        else:
            table_name = model_class_or_name.get_table_name()
        
        query = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{table_name}"
        return self._execute(query)
    
    def insert(self, model_or_table, data=None):
        if isinstance(model_or_table, PieModel):
            table_name = model_or_table.__class__.get_table_name()
            data = model_or_table.to_dict()
        else:
            table_name = model_or_table
        
        columns = list(data.keys())
        values = [self._format_value(data[col]) for col in columns]
        
        columns_str = ", ".join(columns)
        values_str = ", ".join(values)
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        
        return self._execute(query)
    
    def select(self, model_class_or_table, fields=None, conditions=None, as_model=False):
        if isinstance(model_class_or_table, type) and issubclass(model_class_or_table, PieModel):
            table_name = model_class_or_table.get_table_name()
            model_class = model_class_or_table
        else:
            table_name = model_class_or_table
            model_class = None
        
        if fields:
            if isinstance(fields, (list, tuple)):
                fields_str = ", ".join(fields)
            else:
                fields_str = fields
            query = f"SELECT ({fields_str}) FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name}"
        
        if conditions:
            query += f" WHERE {conditions}"
        
        response = self._execute(query)
        result = json.loads(response)
        
        if as_model and model_class:
            return [model_class.from_dict(item) for item in result]
        else:
            return result
    
    def update(self, model_class_or_table, set_values, conditions):
        if isinstance(model_class_or_table, type) and issubclass(model_class_or_table, PieModel):
            table_name = model_class_or_table.get_table_name()
        else:
            table_name = model_class_or_table
        
        
        if isinstance(set_values, dict):
            set_items = [f"{k} = {self._format_value(v)}" for k, v in set_values.items()]
            set_str = ", ".join(set_items)
        else:
            set_str = set_values
        
        query = f"UPDATE {table_name} SET {set_str} WHERE {conditions}"
        
        return self._execute(query)
    
    def delete(self, model_class_or_table, conditions):
        if isinstance(model_class_or_table, type) and issubclass(model_class_or_table, PieModel):
            table_name = model_class_or_table.get_table_name()
        else:
            table_name = model_class_or_table
        
        query = f"DELETE FROM {table_name} WHERE {conditions}"
        
        return self._execute(query)
    
    async def execute_async(self, query):
        return await self._execute_async(query)
    
    async def create_table_async(self, model_class):  
        table_name = model_class.get_table_name()
        field_types = model_class.get_field_types()
        
        
        columns_str = ", ".join([f"{field_name} {field_type}" for field_name, field_type in field_types.items()])
        query = f"CREATE TABLE {table_name} ({columns_str})"
        
        return await self._execute_async(query)
    
    async def check_connection_async(self):    
        return await self._execute_async("CHECK CONNECTION")
    
    async def check_table_async(self, model_class_or_name):   
        if isinstance(model_class_or_name, str):
            table_name = model_class_or_name
        else:
            table_name = model_class_or_name.get_table_name()
        
        query = f"CHECK TABLE {table_name}"
        response = await self._execute_async(query)
        return json.loads(response)
    
    async def drop_table_async(self, model_class_or_name, if_exists=True):     
        if isinstance(model_class_or_name, str):
            table_name = model_class_or_name
        else:
            table_name = model_class_or_name.get_table_name()
        
        query = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{table_name}"
        return await self._execute_async(query)
    
    async def insert_async(self, model_or_table, data=None):     
        if isinstance(model_or_table, PieModel):
            table_name = model_or_table.__class__.get_table_name()
            data = model_or_table.to_dict()
        else:
            table_name = model_or_table
        
        columns = list(data.keys())
        values = [self._format_value(data[col]) for col in columns]
        
        columns_str = ", ".join(columns)
        values_str = ", ".join(values)
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        
        return await self._execute_async(query)
    
    async def select_async(self, model_class_or_table, fields=None, conditions=None, as_model=False):      
        if isinstance(model_class_or_table, type) and issubclass(model_class_or_table, PieModel):
            table_name = model_class_or_table.get_table_name()
            model_class = model_class_or_table
        else:
            table_name = model_class_or_table
            model_class = None
        
        if fields:
            if isinstance(fields, (list, tuple)):
                fields_str = ", ".join(fields)
            else:
                fields_str = fields
            query = f"SELECT ({fields_str}) FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name}"
        
        if conditions:
            query += f" WHERE {conditions}"
        
        response = await self._execute_async(query)
        result = json.loads(response)
        
        if as_model and model_class:
            return [model_class.from_dict(item) for item in result]
        else:
            return result
    
    async def update_async(self, model_class_or_table, set_values, conditions):
        
        if isinstance(model_class_or_table, type) and issubclass(model_class_or_table, PieModel):
            table_name = model_class_or_table.get_table_name()
        else:
            table_name = model_class_or_table
        
        if isinstance(set_values, dict):
            set_items = [f"{k} = {self._format_value(v)}" for k, v in set_values.items()]
            set_str = ", ".join(set_items)
        else:
            set_str = set_values
        
        query = f"UPDATE {table_name} SET {set_str} WHERE {conditions}"
        
        return await self._execute_async(query)
    
    async def delete_async(self, model_class_or_table, conditions):   
        if isinstance(model_class_or_table, type) and issubclass(model_class_or_table, PieModel):
            table_name = model_class_or_table.get_table_name()
        else:
            table_name = model_class_or_table
        
        query = f"DELETE FROM {table_name} WHERE {conditions}"
        
        return await self._execute_async(query)
    
    def __del__(self):
        self.close()
