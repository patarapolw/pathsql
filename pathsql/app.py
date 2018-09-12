import json

from pathsql.databases import case, dictionary as d, webpath


def create_dict_from_case():
    for keyword in case.session.query(case.Keyword).all():
        d_record = d.session.query(d.Dictionary).filter_by(name=keyword.name).first()
        if d_record is None:
            d_record = d.Dictionary()
            d_record.entry = keyword.name

        patho_id = keyword.keyword_patho[0].patho_report.patho_id
        d_case = d.session.query(d.Case).filter_by(patho_id=patho_id).first()
        if d_case is None:
            d_case = d.Case()
            d_case.patho_id = patho_id
            d_case.dictionary_id = d_record.id

            d.session.add(d_case)
            d.session.commit()


def create_dict_from_webpath():
    for entity in webpath.session.query(webpath.Entity).all():
        d_record = d.session.query(d.Dictionary).filter_by(name=entity.name).first()
        if d_record is None:
            d_record = d.Dictionary()
            d_record.entry = entity.name

        for slide in entity.slides:
            data = json.dumps({
                'case_id': slide.case_id,
                'slide_number': slide.slide_number
            }, sort_keys=True)
            d_image = d.session.query(d.Image).filter_by(data=data, type='webpath').first()
            if d_image is None:
                d_image = d.Image()
                d_image.data = data
                d_image.type = 'webpath'
                d_image.dictionary_id = d_record.id

                d.session.add(d_image)
                d.session.commit()


if __name__ == '__main__':
    create_dict_from_case()
