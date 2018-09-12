import os
import json
from datetime import datetime
from urllib.parse import urlparse
import IPython.display

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from citext import CIText

from . import webpath, case, Base, session


class Dictionary(Base):
    __tablename__ = 'dictionary.dictionary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(CIText, unique=True, nullable=False)
    texts = relationship('Text', order_by='Text.modified', back_populates='dictionary')
    images = relationship('Image', order_by='Image.id', back_populates='dictionary')
    cases = relationship('case.PathoReport', order_by='case.PathoReport.patho_id', back_populates='dictionary')
    aliases = relationship('DictAlias', order_by='DictAlias.id', back_populates='dictionary')

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'texts': self.texts,
            'images': self.images,
            'cases': self.cases,
            'aliases': self.aliases
        }

    def __repr__(self):
        return repr(self.to_dict())

    def _repr_html_(self):
        def _default(x):
            try:
                return x.isoformat()
            except AttributeError:
                return repr(x)

        result = self.to_dict()
        result.update({
            'aliases': [alias.alias.name for alias in result['aliases']]
        })

        return '<pre>{}</pre>'.format(json.dumps(result, indent=2, default=_default, ensure_ascii=False))

    @classmethod
    def search(cls, keyword):
        def _search_alias():
            for alias_record in session.query(Alias).filter(Alias.name.ilike('%{}%'.format(keyword))).all():
                da_records = session.query(DictAlias).filter_by(alias_id=alias_record.id).all()
                for da_record in da_records:
                    yield da_record.dictionary

        result = set()
        result.update(_search_alias())
        result.update(session.query(cls).filter(cls.name.ilike('%{}%'.format(keyword))))

        return list(result)

    @property
    def entry(self):
        return [alias.alias.name for alias in self.aliases]

    @entry.setter
    def entry(self, value):
        self.name = value

        session.add(self)
        session.commit()

        self.add_alias(value)

    def add_alias(self, value):
        d_alias = session.query(Alias).filter_by(name=value).first()
        if d_alias is None:
            d_alias = Alias()
            d_alias.name = value

            session.add(d_alias)
            session.commit()

        d_da = DictAlias()
        d_da.dictionary_id = self.id
        d_da.alias_id = d_alias.id

        session.add(d_da)
        session.commit()

        return d_alias

    def group(self, pre_dicts):
        for d in pre_dicts:
            assert isinstance(d, Dictionary)

            for text in d.texts:
                text.dictionary_id = self.id
            for image in d.images:
                image.dictionary_id = self.id
            for case in d.cases:
                case.dictionary_id = self.id
            for dict_alias in d.aliases:
                dict_alias.dictionary_id = self.id

            session.commit()

            if not session.query(Alias).filter_by(name=d.name).first():
                alias = Alias()
                alias.name = d.name

                session.add(alias)
                session.commit()

                dict_alias = DictAlias()
                dict_alias.dictionary_id = self.id
                dict_alias.alias_id = alias.id

                session.add(dict_alias)
                session.commit()

            if d.id != self.id:
                session.delete(d)
                session.commit()


class Text(Base):
    __tablename__ = 'dictionary.text'

    id = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(String(8000), nullable=False)
    created = Column(DateTime, default=datetime.now)
    modified = Column(DateTime, default=datetime.now)

    dictionary_id = Column(Integer, ForeignKey('dictionary.dictionary.id'), nullable=False)
    dictionary = relationship('Dictionary', back_populates='texts')

    def to_dict(self):
        return {
            'id': self.id,
            'text': self.text,
            'created': self.created,
            'modified': self.modified
        }

    def __repr__(self):
        result = self.to_dict()
        result.pop('text')

        return repr(result)

    def _repr_html_(self):
        def _default(x):
            try:
                return x.isoformat()
            except AttributeError:
                return repr(x)

        result = self.to_dict()
        text = result.pop('text')

        return '<pre>{}</pre>'.format(json.dumps(result, indent=2, default=_default, ensure_ascii=False)
                                      + '\n\n' + text)


class Image(Base):
    __tablename__ = 'dictionary.image'

    id = Column(Integer, primary_key=True, autoincrement=True)
    data = Column(String(1000), nullable=False)
    type = Column(CIText, nullable=False)

    dictionary_id = Column(Integer, ForeignKey('dictionary.dictionary.id'), nullable=False)
    dictionary = relationship('Dictionary', back_populates='images')

    def to_dict(self):
        return {
            'id': self.id,
            'data': self.data,
            'type': self.type
        }

    def __repr__(self):
        result = self.to_dict()
        result.pop('data')

        return repr(result)

    def _repr_html_(self):
        def _parse_url(url):
            if not urlparse(url).scheme:
                if not os.path.isabs(url):
                    url = os.path.join('user/image/', url)

                IPython.display.display(IPython.display.Image(filename=url))
            else:
                IPython.display.display(IPython.display.Image(url=url))

        if self.type in ('webpath', 'WebPathology'):
            data = json.loads(self.data)
            wp_record = webpath.Slide()
            wp_record.case_id = data['case_id']
            wp_record.slide_number = data['slide_number']
            return wp_record._repr_html_()
        elif self.type == 'url':
            _parse_url(self.data)

            return ''
        elif self.type == 'json':
            data = json.loads(self.data)
            _parse_url(data['url'])

            return data.get('label', '')
        else:
            return repr(self)


class DictAlias(Base):
    __tablename__ = 'dictionary.dict_alias'

    id = Column(Integer, primary_key=True, autoincrement=True)
    alias = relationship('Alias', back_populates='dict_aliases')

    dictionary_id = Column(Integer, ForeignKey('dictionary.dictionary.id'), nullable=False)
    dictionary = relationship('Dictionary', back_populates='aliases')
    alias_id = Column(Integer, ForeignKey('dictionary.alias.id'), nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'alias': self.alias
        }

    def __repr__(self):
        return repr(self.to_dict())


class Alias(Base):
    __tablename__ = 'dictionary.alias'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(CIText, nullable=False, unique=True)

    dict_aliases = relationship('DictAlias', order_by='DictAlias.id', back_populates='alias')

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name
        }

    def __repr__(self):
        return repr(self.to_dict())
