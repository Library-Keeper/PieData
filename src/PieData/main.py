"""
PieData это БД на основе множества файлов.
Каждая таблица это отдельная папка.
А каждая запись это отдельный файл.
"""

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

class PieModelMeta(type):
    def __new__(self, name, bases, namespace):
        fields = {
            name: field for name, field in namespace.items() if isinstance(field, PieField)
        }
        new_namespace = namespace.copy()
        for name in fields.keys():
            del new_namespace[name]
        new_namespace['_fields'] = fields
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