import tinydb
import os

from pathsql.databases import case


def convert(database_path):
    tdb = tinydb.TinyDB(database_path)
    query = tinydb.Query()
    session = case.Session()

    for record in tdb.table('patho_report').all():
        pr_record = case.PathoReport()
        pr_record.patho_id = record['patho_id']
        pr_record.patient_id = record['hn']
        pr_record.received = record['received']
        pr_record.pathologist = record['pathologist']
        pr_record.resident = record['resident']
        pr_record.report = record['report']

        session.add(pr_record)
        session.commit()

        for keyword in record.get('keywords', list()):
            keyword_record = session.query(case.Keyword).filter_by(name=keyword).first()
            if keyword_record is None:
                keyword_record = case.Keyword()
                keyword_record.name = keyword

                session.add(keyword_record)
                session.commit()

            kp_record = case.KeywordPatho()
            kp_record.patho_id = pr_record.id
            kp_record.keyword_id = keyword_record.id

            session.add(kp_record)
            session.commit()

    for record in tdb.table('patient').all():
        patient_record = case.Patient()
        patient_record.id = record['hn']
        patient_record.full_name = record['full_name']
        patient_record.sex = record['sex']
        patient_record.age = record['age']

        session.add(patient_record)

    session.commit()


if __name__ == '__main__':
    convert(os.path.join('/Users/patarapolw/PycharmProjects/pathology/pathology/case_review',
                         'database.json'))
