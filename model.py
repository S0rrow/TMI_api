from sqlalchemy import (
    Column, String, Text, DateTime, CHAR, ForeignKey,
    create_engine
)
from sqlalchemy.orm import relationship, backref, declarative_base
from sqlalchemy.dialects.mysql import VARCHAR
from datetime import datetime

Base = declarative_base()

# job_information table
class JobInformation(Base):
    __tablename__ = 'job_information'

    pid = Column(VARCHAR(255), primary_key=True, nullable=False)
    job_title = Column(VARCHAR(255), nullable=False)
    site_symbol = Column(VARCHAR(5), nullable=False)
    job_prefer = Column(Text)
    crawl_url = Column(VARCHAR(255))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    post_status = Column(CHAR(1))
    get_date = Column(DateTime, default=datetime.now)
    required_career = Column(CHAR(1))
    resume_required = Column(CHAR(1))
    crawl_domain = Column(VARCHAR(255))
    company_name = Column(VARCHAR(255))
    job_requirements = Column(Text)

    # Relationships
    industries = relationship('IndustryRelation', back_populates='job')
    stacks = relationship('JobStack', back_populates='job')
    categories = relationship('IncludeCategory', back_populates='job')

# industry_relation table
class IndustryRelation(Base):
    __tablename__ = 'industry_relation'

    pid = Column(VARCHAR(255), ForeignKey('job_information.pid', ondelete='CASCADE'), primary_key=True, nullable=False)
    iid = Column(VARCHAR(255), ForeignKey('industry.iid', ondelete='CASCADE'), primary_key=True, nullable=False)

    # Relationships
    job = relationship('JobInformation', back_populates='industries')
    industry = relationship('Industry', back_populates='industry_relations')

# industry table
class Industry(Base):
    __tablename__ = 'industry'

    iid = Column(VARCHAR(255), primary_key=True, nullable=False)
    industry_type = Column(VARCHAR(255), nullable=False)

    # Relationships
    industry_relations = relationship('IndustryRelation', back_populates='industry')

# dev_stack table
class DevStack(Base):
    __tablename__ = 'dev_stack'

    did = Column(VARCHAR(255), primary_key=True, nullable=False)
    dev_stack = Column(VARCHAR(255), nullable=False)

    # Relationships
    job_stacks = relationship('JobStack', back_populates='stack')

# job_stack table
class JobStack(Base):
    __tablename__ = 'job_stack'

    pid = Column(VARCHAR(255), ForeignKey('job_information.pid', ondelete='CASCADE'), primary_key=True, nullable=False)
    did = Column(VARCHAR(255), ForeignKey('dev_stack.did', ondelete='CASCADE'), primary_key=True, nullable=False)

    # Relationships
    job = relationship('JobInformation', back_populates='stacks')
    stack = relationship('DevStack', back_populates='job_stacks')

# cartegory table
class Category(Base):
    __tablename__ = 'category'

    crid = Column(VARCHAR(255), primary_key=True, nullable=False)
    job_category = Column(VARCHAR(255), nullable=False)

    # Relationships
    include_categories = relationship('IncludeCategory', back_populates='category')

# include_cartegory table
class IncludeCategory(Base):
    __tablename__ = 'include_cartegory'

    pid = Column(VARCHAR(255), ForeignKey('job_information.pid', ondelete='CASCADE'), primary_key=True, nullable=False)
    crid = Column(VARCHAR(255), ForeignKey('category.crid', ondelete='CASCADE'), primary_key=True, nullable=False)

    # Relationships
    job = relationship('JobInformation', back_populates='categories')
    category = relationship('Category', back_populates='include_categories')