import tinydb
import json


def get_schema(db_path: str)->dict:
    schema = dict()

    db = tinydb.TinyDB(db_path)
    for table_name in db.tables():
        records = db.table(table_name).all()
        for record in records:
            for k, v in record.items():
                v_types = schema.setdefault(table_name, dict()).get(k, list())
                v_types.append(str(type(v)))
                schema[table_name][k] = list(set(v_types))

    for k, v in schema.items():
        for k2, v2 in v.items():
            if len(v2) == 1:
                schema[k][k2] = v2[0]

    print(json.dumps(schema, indent=2))
    return schema


if __name__ == '__main__':
    import os
    get_schema(os.path.join('/Users/patarapolw/PycharmProjects/pathology/pathology/case_review',
                            'database.json'))
