import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from . import Base


class Entry(Base):
    __tablename__ = 'webpath.entry'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    entities = relationship('Entity', order_by='Entity.name', back_populates='entry')

    case_id = Column(Integer, ForeignKey('webpath.case.id'), nullable=False)
    case = relationship('Case', back_populates='entries')

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'entities': self.entities
        }

    def __repr__(self):
        return repr(self.to_dict())


class Entity(Base):
    __tablename__ = 'webpath.entity'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=False, nullable=False)
    slides = relationship('Slide', order_by='Slide.id', back_populates='entity')

    entry_id = Column(Integer, ForeignKey('webpath.entry.id'), nullable=False)
    entry = relationship('Entry', back_populates='entities')

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'slides': self.slides
        }

    def __repr__(self):
        return repr(self.to_dict())


class Slide(Base):
    __tablename__ = 'webpath.slide'

    id = Column(Integer, primary_key=True, autoincrement=True)
    case_id = Column(Integer, ForeignKey('webpath.case.id'), nullable=False)
    case = relationship('Case', back_populates='slides')
    slide_number = Column(Integer, nullable=False)

    entity_id = Column(Integer, ForeignKey('webpath.entity.id'), nullable=False)
    entity = relationship('Entity', back_populates='slides')

    def to_dict(self):
        return {
            'id': self.id,
            'case_id': self.case_id,
            'case': self.case,
            'slide_number': self.slide_number
        }

    def __repr__(self):
        return repr(self.to_dict())

    def get_slide(self):
        case_id = self.case_id
        slide_no = self.slide_number

        url = 'http://www.webpathology.com/image.asp?case={}&n={}'.format(case_id, slide_no)

        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html5lib')

        parent = soup.find('div', {'id': 'image'})
        content = {
            'case_id': case_id,
            'slide_id': slide_no,
            'title': soup.find('div', {'class': 'span8'}).find('h2').text,
            'label': str(parent.find('p'))
        }

        img = parent.find_next('img', {'class', 'responsive-img'})
        if img is not None:
            content['url'] = urljoin('http://www.webpathology.com/', img['src'])

        return content

    def get_image_url(self):
        return self.get_slide()['url']

    def _repr_html_(self):
        slide_content = self.get_slide()
        return '''<img src="{}" /><br />{}<br /><br />{}'''.format(slide_content['url'],
                                                                   slide_content['title'],
                                                                   slide_content['label'])


class Case(Base):
    __tablename__ = 'webpath.case'

    id = Column(Integer, primary_key=True)
    count = Column(Integer, nullable=False)
    slides = relationship('Slide', back_populates='case')
    entries = relationship('Entry', order_by='Entry.name', back_populates='case')

    def to_dict(self):
        return {
            'id': self.id,
            'count': self.count,
            'slide': self.slides
        }

    def __repr__(self):
        return repr(self.to_dict())
