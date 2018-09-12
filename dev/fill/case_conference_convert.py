import pyexcel

from pathsql.databases import case


def convert(filename):
    iter_row = pyexcel.iget_array(file_name=filename, sheet_name='Slide conference')
    next(iter_row)

    for row in iter_row:
        if not row:
            break

        pr_record = case.session.query(case.PathoReport).filter_by(patho_id=row[3]).first()
        if pr_record is None:
            pr_record = case.PathoReport()
            pr_record.patho_id = row[3]
            pr_record.received = row[1]
            pr_record.type = row[2]

            case.session.add(pr_record)
            case.session.commit()

        kw_record = case.session.query(case.Keyword).filter_by(name=row[4]).first()
        if kw_record is None:
            kw_record = case.Keyword()
            kw_record.name = row[4]

            case.session.add(kw_record)
            case.session.commit()

        kp_record = case.session.query(case.KeywordPatho)\
            .filter_by(patho_id=pr_record.id, keyword_id=kw_record.id).first()
        if kp_record is None:
            kp_record = case.KeywordPatho()
            kp_record.patho_id = pr_record.id
            kp_record.keyword_id = kw_record.id

            case.session.add(kp_record)
            case.session.commit()

    pyexcel.free_resources()


if __name__ == '__main__':
    convert('/Users/patarapolw/Downloads/Academic activities.xlsx')
