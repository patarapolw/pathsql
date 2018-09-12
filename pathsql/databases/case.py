import json

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from citext import CIText

from . import Base, session, dictionary as d


class PathoReport(Base):
    __tablename__ = 'case.patho_report'

    id = Column(Integer, primary_key=True, autoincrement=True)
    patho_id = Column(CIText, nullable=False, unique=True)
    patient_id = Column(Integer, ForeignKey('case.patient.id'), nullable=True)
    patient = relationship('Patient', back_populates='patho_reports')
    received = Column(DateTime, nullable=False)
    pathologist = Column(String(100), nullable=True)
    resident = Column(String(100), nullable=True)
    report = Column(String(8000), nullable=True)
    type = Column(String(20), nullable=True)
    keywords = relationship('KeywordPatho', order_by='KeywordPatho.keyword_id', back_populates='patho_report')

    dictionary_id = Column(Integer, ForeignKey('dictionary.dictionary.id'), nullable=False)
    dictionary = relationship('Dictionary', back_populates='cases')

    def to_dict(self):
        return {
            'id': self.id,
            'patho_id': self.patho_id,
            'patient_id': self.patient_id,
            'patient': self.patient,
            'received': self.received,
            'pathologist': self.pathologist,
            'resident': self.resident,
            'report': self.report,
            'keywords': self.keywords
        }

    def __repr__(self):
        return '{}("{}")'.format(self.__tablename__, self.patho_id)

    def _repr_html_(self):
        result = self.to_dict()
        result['patient'] = getattr(result['patient'], 'to_dict', lambda: None)()
        result['received'] = result['received'].isoformat()
        result['keywords'] = [kp.keyword.name for kp in result['keywords']]
        report = result.pop('report')
        if report is None:
            report = ''

        return '<pre>{}</pre>'.format(json.dumps(result, ensure_ascii=False, indent=2)
                                      + '\n\n' + report)

    def add_keyword(self, keyword):
        keyword_record = session.query(Keyword).filter_by(name=keyword).first()
        if keyword_record is None:
            keyword_record = Keyword()
            keyword_record.name = keyword

            session.add(keyword_record)
            session.commit()

        kp_record = KeywordPatho()
        kp_record.patho_id = self.id
        kp_record.keyword_id = keyword_record.id

        session.add(kp_record)
        session.commit()

        return 'Added'

    def remove_keyword(self, keyword):
        keyword_record = session.query(Keyword).filter_by(name=keyword).first()
        if keyword_record is not None:
            kp_record = session.query(KeywordPatho).filter_by(patho_id=self.id, keyword_id=keyword_record.id).first()
            if kp_record is not None:
                session.delete(kp_record)
                session.commit()

                return 'Deleted'

    def get_keywords(self):
        return [kp.keyword.name for kp in self.keywords]

    @classmethod
    def search_keyword(cls, keyword):
        for keyword_record in session.query(Keyword).filter(Keyword.name.ilike('%{}%'.format(keyword))).all():
            kp_records = session.query(KeywordPatho).filter_by(keyword_id=keyword_record.id).all()
            for kp_record in kp_records:
                yield kp_record.patho_report

    @classmethod
    def search(cls, keyword):
        result = set()
        result.update(cls.search_keyword(keyword))
        result.update(session.query(cls).filter(cls.report.ilike('%{}%'.format(keyword))))

        return list(result)


class Patient(Base):
    __tablename__ = 'case.patient'

    id = Column(Integer, primary_key=True)
    full_name = Column(String(100), nullable=False, unique=True)
    sex = Column(String(20), nullable=True)
    age = Column(String(100), nullable=True)
    patho_reports = relationship('PathoReport', order_by='PathoReport.patho_id', back_populates='patient')

    def to_dict(self):
        return {
            'id': self.id,
            'full_name': self.full_name,
            'sex': self.sex,
            'age': self.age,
            'patho_reports': [report.patho_id for report in self.patho_reports]
        }

    def __repr__(self):
        return '{}({})'.format(self.__tablename__, self.id)


class KeywordPatho(Base):
    __tablename__ = 'case.keyword_patho'

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = relationship('Keyword', back_populates='keyword_patho')

    patho_id = Column(Integer, ForeignKey('case.patho_report.id'), nullable=False)
    keyword_id = Column(Integer, ForeignKey('case.keyword.id'), nullable=False)

    patho_report = relationship('PathoReport', back_populates='keywords')

    def to_dict(self):
        return {
            'id': self.id,
            'keyword': self.keyword
        }

    def __repr__(self):
        return repr(self.to_dict())


class Keyword(Base):
    __tablename__ = 'case.keyword'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(CIText, nullable=False, unique=True)

    keyword_patho = relationship('KeywordPatho', order_by='KeywordPatho.id', back_populates='keyword')

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name
        }

    def __repr__(self):
        return repr(self.to_dict())

    @classmethod
    def no_connection(cls):
        for kw_record in session.query(cls).all():
            if not kw_record.keyword_patho:
                yield kw_record
