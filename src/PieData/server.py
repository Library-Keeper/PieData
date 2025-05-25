from functools import partial
import os
import shutil
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
import json
import re
import asyncio
import websockets
from pathlib import Path
import platform

def load_config():
    default = {
        "host": "127.0.0.1",
        "port": 8765,
        "db_path": str(Path.home() / "PieDataDB")
    }

    if platform.system() == "Windows":
        config_dir = Path(os.getenv("APPDATA")) / "PieDataServer"
    else:
        config_dir = Path.home() / ".PieDataServer"

    config_file = config_dir / "config.json"
    config_dir.mkdir(parents=True, exist_ok=True)

    if config_file.exists():
        with open(config_file, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(default, f, indent=4)
        return default


def parse_value(value: str):
    value = value.strip()
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value

def parse_sql(sql: str):
    sql = re.sub(r"\s+", " ", sql.strip())
    sql_lower = sql.lower()
    try:
        if sql_lower.startswith("create table"):
            match = re.match(r"create table (\w+) \((.*)\)", sql, re.IGNORECASE)
            table_name = match.group(1)
            columns = []
            for col_def in match.group(2).split(","):
                name, type_ = re.match(r"\s*(\w+)\s+(\w+)\s*", col_def.strip()).groups()
                columns.append((name, type_.capitalize()))
            return ("CREATE_TABLE", {"table_name": table_name, "columns": columns})

        elif sql_lower.startswith("insert into"):
            match = re.match(r"insert into (\w+) \((.*?)\) values \((.*?)\)", sql, re.IGNORECASE)
            table_name = match.group(1)
            columns = [c.strip() for c in match.group(2).split(",")]
            values = [parse_value(v.strip()) for v in match.group(3).split(",")]
            data = {col: val for col, val in zip(columns, values)}
            return ("INSERT", {"table_name": table_name, "data": data})

        elif sql_lower.startswith("select"):
            match_all = re.match(r"select\s+\*\s+from\s+(\w+)(?:\s+where\s+(.*))?", sql, re.IGNORECASE)
            if match_all:
                table_name = match_all.group(1)
                conditions = []
                if match_all.group(2):
                    for cond in match_all.group(2).split(" and "):
                        col, op, val = re.match(r"(\w+)\s*(!=|=|>=|<=|>|<)\s*(.*)", cond).groups()
                        conditions.append((col, op, val))
                return ("SELECT", {"table_name": table_name, "fields": None, "conditions": conditions})
            
            match_fields = re.match(r"select\s+\(([^)]+)\)\s+from\s+(\w+)(?:\s+where\s+(.*))?", sql, re.IGNORECASE)
            if match_fields:
                fields = [f.strip() for f in match_fields.group(1).split(",")]
                table_name = match_fields.group(2)
                conditions = []
                if match_fields.group(3):
                    for cond in match_fields.group(3).split(" and "):
                        col, op, val = re.match(r"(\w+)\s*(!=|=|>=|<=|>|<)\s*(.*)", cond).groups()
                        conditions.append((col, op, val))
                return ("SELECT", {"table_name": table_name, "fields": fields, "conditions": conditions})
            
            raise Exception("Неверный синтаксис SELECT-запроса")

        elif sql_lower.startswith("update"):
            match = re.match(r"update (\w+) set (.*?) where (.*)", sql, re.IGNORECASE)
            table_name = match.group(1)
            set_values = {}
            for pair in match.group(2).split(","):
                k, v = pair.split("=", 1)
                set_values[k.strip()] = parse_value(v.strip())
            conditions = []
            for cond in match.group(3).split(" and "):
                col, op, val = re.match(r"(\w+)\s*(!=|=|>=|<=|>|<)\s*(.*)", cond).groups()
                val_parsed = parse_value(val)
                conditions.append((col, op, val_parsed))
            return ("UPDATE", {"table_name": table_name, "set_values": set_values, "conditions": conditions})

        elif sql_lower.startswith("delete from"):
            match = re.match(r"delete from (\w+)(?: where (.*))?", sql, re.IGNORECASE)
            table_name = match.group(1)
            conditions = []
            if match.group(2):
                for cond in match.group(2).split(" and "):
                    col, op, val = re.match(r"(\w+)\s*(!=|=|>=|<=|>|<)\s*(.*)", cond).groups()
                    val_parsed = parse_value(val)
                    conditions.append((col, op, val_parsed))
            return ("DELETE", {"table_name": table_name, "conditions": conditions})

        elif sql_lower.startswith("drop table"):
            match = re.match(r"drop table (if exists)?(.*)", sql, re.IGNORECASE)
            table_name = match.group(2).strip()
            return ("DROP TABLE", {"table_name": table_name, "if_exists": bool(match.group(1))})
        
        elif sql_lower.startswith("check"):
            match = re.match(r"check (connection|table) ?(\w*)?", sql, re.IGNORECASE)
            type_, table_name = match.groups()
            return ("CHECK", {"type": type_, "table_name": table_name})

        else:
            raise Exception("Unsupported SQL command")
    except Exception as e:
        raise Exception(f"SQL parsing error: {str(e)}")

class Database:
    def __init__(self, root_dir="database"):
        self.root_dir = root_dir
        os.makedirs(root_dir, exist_ok=True)

    def create_table(self, table_name, columns):
        table_path = os.path.join(self.root_dir, table_name)
        if os.path.exists(table_path):
            raise Exception(f"Table {table_name} exists")
        os.makedirs(table_path)
        meta = [{"name": col[0], "type": col[1]} for col in columns]

        root = ET.Element("table", name=table_name)

        for m in meta:
            col = ET.SubElement(root, "column", name=m["name"], type=m["type"])
        
        ET.ElementTree(root).write(os.path.join(table_path, "columns.xml"), encoding="utf-8", xml_declaration=True)

    def _get_columns(self, table_name):
        table_path = os.path.join(self.root_dir, table_name)
        meta_file = os.path.join(table_path, "columns.xml")

        tree = ET.parse(meta_file)
        root = tree.getroot()
        res = []
        for col in root.findall("column"):
            res.append({"name": col.items()[0][1], "type": col.items()[1][1]})
        if not os.path.exists(meta_file):
            raise Exception(f"Table {table_name} missing")
        return res

    def _validate_data(self, table_name, data):
        columns = self._get_columns(table_name)
        col_types = {col["name"]: col["type"] for col in columns}
        for key, value in data.items():
            if key not in col_types:
                raise Exception(f"Column {key} not in {table_name}")
            expected_type = col_types[key]
            if expected_type == "Integer" and not isinstance(value, int | None):
                raise TypeError(f"{key} must be Integer")
            elif expected_type == "String" and not isinstance(value, str | None):
                raise TypeError(f"{key} must be String")
            elif expected_type == "Datetime" and not isinstance(value, datetime | None):
                raise TypeError(f"{key} must be Datetime")
            elif expected_type == "Float" and not isinstance(value, float | int | None):
                raise TypeError(f"{key} must be Float")
        return data

    def insert_into(self, table_name, data):
        columns = self._get_columns(table_name)
        col_types = {col["name"]: col["type"] for col in columns}
        parsed_data = {}
        for key, value in data.items():
            parsed_data[key] = self._parse_value(col_types[key], value)
        self._validate_data(table_name, parsed_data)
        for key in col_types.keys():
            if key not in parsed_data.keys():
                parsed_data[key] = None
        record_id = str(uuid.uuid4())
        root = ET.Element("record")
        for key, value in parsed_data.items():
            col = ET.SubElement(root, "column", name=key, type=col_types[key])
            if isinstance(value, datetime):
                col.text = value.isoformat()
            else:
                col.text = str(value)
        file_path = os.path.join(self.root_dir, table_name, f"{record_id}-piedb.xml")
        ET.ElementTree(root).write(file_path, encoding="utf-8", xml_declaration=True)
        return record_id

    def _get_column_type(self, table_name, column_name):
        for col in self._get_columns(table_name):
            if col["name"] == column_name:
                return col["type"]
        raise Exception(f"Column {column_name} not found")

    def _parse_value(self, col_type, value):
        if (
            (col_type == "Integer" and isinstance(value, int | None)) or
            (col_type == "Float" and isinstance(value, float | int | None)) or
            (col_type == "String" and isinstance(value, str | None)) or
            (col_type == "Datetime" and isinstance(value, datetime | None))
        ):
            return value
        
        value_str = str(value).strip("'\"") if isinstance(value, str) else str(value)
        try:
            if value_str == "None":
                return "None"
            elif col_type == "Integer":
                return int(value_str)
            elif col_type == "Float":
                return float(value_str)
            elif col_type == "String":
                return value_str
            elif col_type == "Datetime":
                return datetime.fromisoformat(value_str)
            else:
                raise Exception(f"Unknown type {col_type}")
        except ValueError as e:
            raise ValueError(f"Cannot convert {value} to {col_type}: {e}")

    def _parse_conditions(self, table_name, raw_conditions):
        conditions = []
        for col, op, val in raw_conditions:
            col_type = self._get_column_type(table_name, col)
            parsed_val = self._parse_value(col_type, val)
            if op == "=": 
                conditions.append(lambda r, c=col, v=parsed_val: r[c] == v)
            elif op == "!=": 
                conditions.append(lambda r, c=col, v=parsed_val: r[c] != v)
            elif op == ">": 
                conditions.append(lambda r, c=col, v=parsed_val: r[c] > v)
            elif op == "<": 
                conditions.append(lambda r, c=col, v=parsed_val: r[c] < v)
            elif op == ">=": 
                conditions.append(lambda r, c=col, v=parsed_val: r[c] >= v)
            elif op == "<=": 
                conditions.append(lambda r, c=col, v=parsed_val: r[c] <= v)
            else: 
                raise Exception(f"Unsupported operator {op}")
        return conditions

    def select_from(self, table_name, fields=None, conditions=None):
        records = []
        for filename in os.listdir(os.path.join(self.root_dir, table_name)):
            if not filename.endswith("-piedb.xml"):
                continue
            file_path = os.path.join(self.root_dir, table_name, filename)
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                full_record = {}
                for col in root.findall("column"):
                    name = col.attrib["name"]
                    col_type = col.attrib["type"]
                    value = self._parse_value(col_type, col.text)
                    full_record[name] = value
                
                # Проверяем условия на полной записи
                if not conditions or all(cond(full_record) for cond in conditions):
                    # Если fields=None, возвращаем все поля, иначе только указанные
                    if fields:
                        record = {k: v for k, v in full_record.items() if k in fields}
                    else:
                        record = full_record
                    records.append(record)
            except Exception as e:
                print(f"Ошибка обработки записи: {e}")
        return records

    def update(self, table_name, set_values, conditions=None):
        updated = 0
        columns = self._get_columns(table_name)
        col_types = {col["name"]: col["type"] for col in columns}
        parsed_set_values = {}
        for key, value in set_values.items():
            parsed_set_values[key] = self._parse_value(col_types[key], value)
        
        for filename in os.listdir(os.path.join(self.root_dir, table_name)):
            if not filename.endswith("-piedb.xml"):
                continue
            file_path = os.path.join(self.root_dir, table_name, filename)
            tree = ET.parse(file_path)
            root = tree.getroot()
            record = {col.attrib["name"]: self._parse_value(col.attrib["type"], col.text) 
                     for col in root.findall("column")}
            if conditions and not all(cond(record) for cond in conditions):
                continue
            for key, value in parsed_set_values.items():
                for col in root.findall(f"column[@name='{key}']"):
                    if isinstance(value, datetime):
                        col.text = value.isoformat()
                    else:
                        col.text = str(value)
            tree.write(file_path, encoding="utf-8", xml_declaration=True)
            updated += 1
        return updated

    def delete_from(self, table_name, conditions=None):
        deleted = 0
        for filename in os.listdir(os.path.join(self.root_dir, table_name)):
            if not filename.endswith("-piedb.xml"):
                continue
            file_path = os.path.join(self.root_dir, table_name, filename)
            tree = ET.parse(file_path)
            root = tree.getroot()
            record = {col.attrib["name"]: self._parse_value(col.attrib["type"], col.text) 
                     for col in root.findall("column")}
            if not conditions or all(cond(record) for cond in conditions):
                os.remove(file_path)
                deleted += 1
        return deleted
    
    def drop_table(self, table_name, if_exists):
        table_path = os.path.join(self.root_dir, table_name)
        if not os.path.exists(table_path):
            if not if_exists:
                raise FileNotFoundError(f"Table directory '{table_path}' does not exist")
            return

        try:
            shutil.rmtree(table_path)
        except OSError as e:
            raise OSError(f"Failed to remove table directory '{table_path}': {e}")

        return table_name

async def handle_query(query, db: Database):
    try:
        action, params = parse_sql(query)
        if action == "CREATE_TABLE":
            db.create_table(params["table_name"], params["columns"])
            return "Table created successfully"
        
        elif action == "INSERT":
            record_id = db.insert_into(params["table_name"], params["data"])
            return f"Inserted: {record_id}"
        
        elif action == "SELECT":
            conditions = db._parse_conditions(params["table_name"], params.get("conditions", []))
            fields = params.get("fields")
            records = db.select_from(params["table_name"], fields, conditions)
            return json.dumps(records, default=str)

        elif action == "UPDATE":
            updated = db.update(params["table_name"], params["set_values"], 
                              db._parse_conditions(params["table_name"], params["conditions"]))
            return f"Updated: {updated}"
        
        elif action == "DELETE":
            deleted = db.delete_from(params["table_name"], 
                                   db._parse_conditions(params["table_name"], params["conditions"]))
            return f"Deleted: {deleted}"
        
        elif action == "DROP TABLE":
            drop = db.drop_table(params["table_name"], params["if_exists"])
            return f"Droped table: {drop}"
        
        elif action == "CHECK":
            if params["type"].lower() == "connection":
                return "Connection Success"
            elif params["type"].lower() == "table":
                try:
                    db._get_columns(params["table_name"])
                    return json.dumps((True, f"Table '{params["table_name"]}' exists"), default=str)
                except:
                    return json.dumps((False, f"Table '{params["table_name"]}' not exists"), default=str)

        else:
            return "Unknown action"
    except Exception as e:
        return f"Error: {str(e)}"

async def server(websocket, db : Database, path=None):
    try:
        async for message in websocket:
            response = await handle_query(message, db=db)
            await websocket.send(response)
    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        print(f"Server error: {e}")
    finally:
        if websocket.state != websockets.State.CLOSED:
            await websocket.close()

async def main_async():
    
    cfg = load_config()
    host = cfg["host"]
    port = cfg["port"]
    path = cfg["db_path"]
    db = Database(path)
    handler = partial(server, db=db)
    async with websockets.serve(handler, host, port):
        await asyncio.Future()

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
