import tinydb
import os

from pathsql.databases import webpath


def convert(database_path):
    tdb = tinydb.TinyDB(database_path)
    query = tinydb.Query()
    session = webpath.Session()

    for record in tdb.all():
        print(record)

        case = tdb.table('case').get(doc_id=record['case'])

        entry_record = webpath.Entry()
        entry_record.name = record['entry']
        entry_record.case_id = case['case_id']

        session.add(entry_record)

    session.commit()

    for record in tdb.table('entity').all():
        print(record)

        entries = tdb.search(query.entity.test(lambda x: record.doc_id in x))
        if len(entries) > 0:
            entry_record = session.query(webpath.Entry).filter_by(name=entries[0]['entry']).first()

            entity_record = webpath.Entity()
            entity_record.name = record['entry']
            entity_record.entry_id = entry_record.id

            session.add(entity_record)
            session.commit()

            for slide in record['entity']:
                slide_record = webpath.Slide()
                slide_record.case_id = slide['case_id']
                slide_record.slide_number = slide['slide_id']
                slide_record.entity_id = entity_record.id

                session.add(slide_record)

            session.commit()

    for record in tdb.table('case').all():
        print(record)

        case_record = webpath.Case()
        case_record.id = record['case_id']
        case_record.count = record['count']

        session.add(case_record)

    session.commit()


if __name__ == '__main__':
    convert(os.path.join('/Users/patarapolw/PycharmProjects/pathology/pathology/webpathology',
                         'database.json'))
