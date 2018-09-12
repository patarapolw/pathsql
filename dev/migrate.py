import sqlite3
import dateutil.parser

from pathsql.databases import (Base, engine,
                               session, case, dictionary as d)


def main():
    conn = {
        'case': sqlite3.connect('../pathsql/databases/case.db'),
        'dictionary': sqlite3.connect('../pathsql/databases/dictionary.db'),
        'webpath': sqlite3.connect('../pathsql/databases/webpath.db')
    }

    for v in conn.values():
        v.row_factory = sqlite3.Row

    try:
        for old_d_dict in conn['dictionary'].execute('SELECT * FROM dictionary'):
            d_dict = session.query(d.Dictionary).filter_by(name=old_d_dict['name']).first()
            if d_dict is None:
                d_dict = d.Dictionary()
                d_dict.name = old_d_dict['name']

                session.add(d_dict)
                session.commit()

            for old_d_text in conn['dictionary'].execute('SELECT * FROM text WHERE dictionary_id=?',
                                                         (old_d_dict['id'],)):
                d_text = session.query(d.Text).filter_by(text=old_d_text['text']).first()
                if d_text is None:
                    d_text = d.Text()
                    d_text.text = old_d_text['text']
                    d_text.created = dateutil.parser.parse(old_d_text['created'])
                    d_text.modified = dateutil.parser.parse(old_d_text['modified'])
                    d_text.dictionary_id = d_dict.id

                    session.add(d_text)
            session.commit()

            for old_d_image in conn['dictionary'].execute('SELECT * FROM image WHERE dictionary_id=?',
                                                          (old_d_dict['id'],)):
                d_image = session.query(d.Image).filter_by(data=old_d_image['data'],
                                                           type=old_d_image['type']).first()
                if d_image is None:
                    d_image = d.Image()
                    d_image.data = old_d_image['data']
                    d_image.type = old_d_image['type']
                    d_image.dictionary_id = d_dict.id

                    session.add(d_image)
            session.commit()

            for old_d_da in conn['dictionary'].execute('SELECT alias_id FROM dict_alias WHERE dictionary_id=?',
                                                       (old_d_dict['id'],)):
                for old_d_alias in conn['dictionary'].execute('SELECT * FROM alias WHERE id=?',
                                                              (old_d_da[0],)):
                    d_alias = session.query(d.Alias).filter_by(name=old_d_alias['name']).first()
                    if d_alias is None:
                        d_alias = d.Alias()
                        d_alias.name = old_d_alias['name']

                        session.add(d_alias)
                        session.commit()

                    d_da = session.query(d.DictAlias).filter_by(dictionary_id=d_dict.id, alias_id=d_alias.id).first()
                    if d_da is None:
                        d_da = d.DictAlias()
                        d_da.dictionary_id = d_dict.id
                        d_da.alias_id = d_alias.id

                        session.add(d_da)
                        session.commit()

            for old_d_case in conn['dictionary'].execute('SELECT patho_id FROM "case" WHERE dictionary_id=?',
                                                         (old_d_dict['id'],)):
                for old_case_pr in conn['case'].execute('SELECT * FROM patho_report WHERE patho_id=?',
                                                        (old_d_case[0], )):
                    for old_case_patient in conn['case'].execute('SELECT * FROM patient WHERE id=?',
                                                                 (old_case_pr['patient_id'],)):
                        case_patient = session.query(case.Patient).filter_by(id=old_case_pr['patient_id']).first()
                        if case_patient is None:
                            case_patient = case.Patient()
                            case_patient.id = old_case_pr['patient_id']
                            case_patient.full_name = old_case_patient['full_name']
                            case_patient.sex = old_case_patient['sex']
                            case_patient.age = old_case_patient['age']

                            session.add(case_patient)
                            session.commit()

                    case_pr = session.query(case.PathoReport).filter_by(patho_id=old_case_pr['patho_id']).first()
                    if case_pr is None:
                        case_pr = case.PathoReport()
                        case_pr.patho_id = old_case_pr['patho_id']
                        case_pr.patient_id = old_case_pr['patient_id']
                        case_pr._received = dateutil.parser.parse(old_case_pr['_received'])
                        case_pr.pathologist = old_case_pr['pathologist']
                        case_pr.resident = old_case_pr['resident']
                        case_pr.report = old_case_pr['report']
                        case_pr.type = old_case_pr['type']
                        case_pr.dictionary_id = d_dict.id

                        session.add(case_pr)
                        session.commit()

                    for old_case_kp in conn['case'].execute('SELECT keyword_id FROM keyword_patho WHERE patho_id=?',
                                                            (old_case_pr['id'],)):
                        for old_case_keyword in conn['case'].execute('SELECT * FROM keyword WHERE id=?',
                                                                     (old_case_kp[0],)):
                            case_keyword = session.query(case.Keyword).filter_by(name=old_case_keyword['name']).first()
                            if case_keyword is None:
                                case_keyword = case.Keyword()
                                case_keyword.name = old_case_keyword['name']

                                session.add(case_keyword)
                                session.commit()

                            case_kp = session.query(case.KeywordPatho).filter_by(keyword_id=case_keyword.id,
                                                                                 patho_id=case_pr.id).first()
                            if case_kp is None:
                                case_kp = case.KeywordPatho()
                                case_kp.keyword_id = case_keyword.id
                                case_kp.patho_id = case_pr.id

                                session.add(case_kp)
                                session.commit()
    finally:
        for v in conn.values():
            v.close()


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    main()
