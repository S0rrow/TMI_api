import os, json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from sqlalchemy import create_engine, or_, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from utils import Logger
from datetime import datetime
import ast
from model import JobInformation, DevStack, JobStack, Category, IncludeCategory, Industry, IndustryRelation

parent_path = os.path.dirname(os.path.abspath(__file__))
config_path = f"{parent_path}/config.json"
app = FastAPI()
logger = Logger()

### input models
class QueryCall(BaseModel):
    database: str
    query : str

class UniqueValuesCall(BaseModel):
    database:str
    table:str
    column:str
    is_stacked:bool

class MetaDataCall(BaseModel):
    database:str
    table:str

### API calls
@app.post("/query")
def query(input:QueryCall):
    method_name = __name__ + ".query"
    logger.log(f"api called", flag=0, name=method_name)
    try:
        df = query_to_dataframe(input.database, input.query)
        serialized_df = df.astype(object).to_dict(orient='records')
        return serialized_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred while querying from database: {e}")

@app.post("/unique_values")
def retrieve_unique_values(input: UniqueValuesCall):
    method_name = __name__ + ".retrieve_unique_values"
    logger.log(f"api called", flag=0, name=method_name)
    database = input.database
    table = input.table
    column = input.column
    is_stacked = input.is_stacked
    separator = -1

    try:
        engine = create_db_engine(database)
        Session = sessionmaker(bind=engine)
        session = Session()

        if is_stacked:
            separator = 0
            # Handle stacked column data
            model = get_model_from_table(table)
            query = session.query(getattr(model, column)).filter(
                getattr(model, column) != '[]',
                getattr(model, column) != "['null']"
            ).all()
            all_elems = []
            separator = 1
            for row in query:
                try:
                    col_elem_list = ast.literal_eval(row[0])  # row is a tuple, so row[0] accesses the column data
                except (SyntaxError, ValueError) as e:
                    logger.log(f"Error parsing row: {row}, error: {e}", flag=1, name=method_name)
                    continue  # Skip the erroneous row
                all_elems.extend(col_elem_list)
            unique_stacked_elem_list = list(set(all_elems))
            return {"unique_values": unique_stacked_elem_list}
        else:
            separator = 2
            # Handle distinct values from a column
            model = get_model_from_table(table)
            query = session.query(getattr(model, column)).distinct().all()
            separator = 3
            unique_elem_list = list(set(row[0] for row in query))
            return {"unique_values": unique_elem_list}
    except Exception as e:
        logger.log(f"Exception occurred while retrieving unique values from table on separator #{separator}: {e}", flag=1, name=method_name)
        raise HTTPException(status_code=500, detail=f"Exception occurred while retrieving unique values from table: {e}")

@app.post("/columns")
def get_columns(input:MetaDataCall):
    method_name = __name__ + ".get_columns"
    logger.log(f"api called", flag=0, name=method_name)
    database = input.database
    table_name = input.table
    try:
        engine = create_db_engine(database)
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        column_names = [col.name for col in table.columns]
        return {"column_names":column_names}
    except Exception as e:
        logger.log(f"",flag=1, name=method_name)
        raise HTTPException(status_code=500, detail=f"Exception occurred while retrieving metadata of table:{e}")

@app.post("/row_count")
def get_table_row_count(input:QueryCall):
    method_name = __name__ + ".get_table_row_count"
    logger.log(f"api called", flag=0, name=method_name)
    try:
        database = input.database
        query = input.query
        engine = create_db_engine(database)
        # 연결하고 쿼리 실행
        with engine.connect() as connection:
            result = connection.execute(text(query))
            row_count = result.scalar()  # 첫 번째 결과 값을 가져옴
            return {"row_count": row_count}
    except Exception as e:
        logger.log(f"Exception occurred while getting table row count: {e}", flag=1, name=method_name)
        raise HTTPException(status_code=500, detail=f"Error retrieving row count: {e}")

@app.get("/stacked_columns")
def get_stacked_columns(database:str, table:str):
    method_name = __name__ + ".get_stacked_columns"
    logger.log(f"api called", flag=0, name=method_name)
    try:
        query = f"SELECT * FROM {table} LIMIT 100;"
        result_df = query_to_dataframe(database, query)
        stacked_columns = [col for col in result_df.columns if isinstance(result_df[col].iloc[0], str) and result_df[col].iloc[0].startswith('[')]
        # logger.log(f"Retrieved data: {result_df}", flag=0, name=method_name)  # debugging log added
        # stacked_columns = [col for col in result_df.columns if str(result_df[col].iloc[0]).startswith('[')]
        return {"stacked_columns":stacked_columns}
    except Exception as e:
        logger.log(f"Exception occurred while getting stacked columns as list: {e}", flag=1, name=method_name)
        raise HTTPException(status_code=500, detail=f"Exception occurred while getting stacked columns as list: {e}")

@app.get("/dev_stacks")
def get_dev_stacks(database:str):
    method_name = __name__ + ".get_dev_stacks"
    logger.log(f"api called", flag=0, name=method_name)
    try:
        query = """
            SELECT T2.dev_stack FROM job_stack T1 INNER JOIN dev_stack T2 ON T1.did = T2.did
        """
        result_df = query_to_dataframe(database=database, query=query)
        dev_stacks = result_df['dev_stack'].tolist()
        return {"dev_stacks": dev_stacks}
    except Exception as e:
        logger.log(f"Exception occurred while getting dev stacks: {e}", flag=1, name=method_name)
        raise HTTPException(status_code=500, detail=f"Exception occurred while getting dev stacks: {e}")

@app.get("/search_keyword")
def get_search_results(database: str, search_keyword: str):
    method_name = __name__ + ".get_search_result"
    logger.log(f"api called", flag=0, name=method_name)
    try:
        engine = create_db_engine(database)
        Session = sessionmaker(bind=engine)
        session = Session()

        if search_keyword:
            # Define the common columns for all queries
            columns = [JobInformation.pid, JobInformation.job_title, JobInformation.site_symbol,
                       JobInformation.crawl_url, JobInformation.crawl_domain, JobInformation.company_name]
            
            # Create individual queries with the same columns
            query1 = session.query(*columns).filter(
                or_(
                    JobInformation.job_title.like(f"%{search_keyword}%"),
                    JobInformation.site_symbol.like(f"%{search_keyword}%"),
                    JobInformation.crawl_url.like(f"%{search_keyword}%"),
                    JobInformation.crawl_domain.like(f"%{search_keyword}%"),
                    JobInformation.company_name.like(f"%{search_keyword}%"),
                )
            )
            
            query2 = session.query(JobInformation.pid, JobInformation.job_title, JobInformation.site_symbol,
                                   JobInformation.crawl_url, JobInformation.crawl_domain, JobInformation.company_name).join(
                JobStack
            ).join(
                DevStack
            ).filter(
                DevStack.dev_stack.like(f"%{search_keyword}%")
            )

            query3 = session.query(JobInformation.pid, JobInformation.job_title, JobInformation.site_symbol,
                                   JobInformation.crawl_url, JobInformation.crawl_domain, JobInformation.company_name).join(
                IncludeCategory
            ).join(
                Category
            ).filter(
                Category.job_category.like(f"%{search_keyword}%")
            )

            query4 = session.query(JobInformation.pid, JobInformation.job_title, JobInformation.site_symbol,
                                   JobInformation.crawl_url, JobInformation.crawl_domain, JobInformation.company_name).join(
                IndustryRelation
            ).join(
                Industry
            ).filter(
                Industry.industry_type.like(f"%{search_keyword}%")
            )

            # Combine queries using union
            combined_query = query1.union(query2).union(query3).union(query4)
        else:
            # Select all columns if no search keyword is provided
            combined_query = session.query(JobInformation.pid, JobInformation.job_title, JobInformation.site_symbol,
                                           JobInformation.crawl_url, JobInformation.crawl_domain, JobInformation.company_name)

        # Execute query and get results
        result_proxy = combined_query.all()

        # Convert results to DataFrame
        df = pd.DataFrame(result_proxy, columns=['pid', 'job_title', 'site_symbol', 'crawl_url', 'crawl_domain', 'company_name'])
        result_pid_list = df['pid'].to_list()
        #serialized_df = df.astype(object).to_dict(orient='records')
        #return serialized_df
        return {"result":result_pid_list}
    except Exception as e:
        logger.log(f"Exception occurred while getting search results: {e}", flag=1, name=method_name)
        raise HTTPException(status_code=500, detail=f"Exception occurred while getting search results: {e}")

@app.get("/job_information")
def get_job_information(database: str, pid_list: str):
    method_name = __name__ + ".get_job_information"
    logger.log(f"api called", flag=0, name=method_name)
    try:
        pid_list = ast.literal_eval(pid_list)
        engine = create_db_engine(database)
        Session = sessionmaker(bind=engine)
        session = Session()
        result = {}

        jobs = session.query(JobInformation).filter(JobInformation.pid.in_(pid_list)).all()

        for job in jobs:
            job_data = {
                "job_title": job.job_title,
                "company_name": job.company_name,
                "dev_stacks": [stack.stack.dev_stack for stack in job.stacks],  # dev stack list
                "required_career": job.required_career,
                "start_date": job.start_date,
                "end_date": job.end_date
            }
            result[job.pid] = job_data
        return result

    except Exception as e:
        logger.log(f"Exception occurred while getting job information: {e}", flag=1, name=method_name)
        raise HTTPException(status_code=500, detail=f"Exception occurred while getting job information: {e}")


### methods
def load_config(config_path:str='config.json')->dict:
    """return configuration informations from config.json"""
    with open(config_path, 'r') as f:
        return json.load(f)

def create_db_engine(database:str, config=None):
    """generate db engine through configuration file."""
    method_name = __name__ + ".create_db_engine"
    if config is None:
        config = load_config()
    try:
        user = config.get("USER")
        password = config.get("PASSWORD")
        host = config.get("ENDPOINT")
        port = config.get("PORT")
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(connection_string)
    except Exception as e:
        logger.log(f"Exception occurred while creating db engine: {e}", flag=1, name=method_name)
        raise e
    
def execute_query(database:str, query:str, params:dict=None)->bool:
    """
    Execute SQL query and return True if successful, False otherwise.
    - database: database name to connect
    - query: SQL query to execute
    - params: parameters for the query (optional)
    - config_path: path to config.json
    """
    method_name = __name__ + ".execute_query"
    try:
        engine = create_db_engine(database)
        
        with engine.connect() as connection:
            try:
                if params:
                    connection.execute(text(query), params)
                else:
                    connection.execute(text(query))
                connection.commit()

            except Exception as e:
                logger.log(f"Exception occurred while executing query: {e}", flag=1, name=method_name)
                return False
        return True
    except Exception as e:
        logger.log(f"Exception occurred while executing query: {e}", flag=1, name=method_name)
        return Exception(e)
    

def query_to_dataframe(database:str, query:str)->pd.DataFrame:
    """
        execute sql query and return results in dataframe.
        - database: database name to connect
        - query: sql query to execute
        - config_path: path to config.json
    """
    method_name = __name__ + ".query_to_dataframe"
    try:
        engine = create_db_engine(database)
        with engine.connect() as connection:
            try:
                df = pd.read_sql(query, connection)
            except Exception as e:
                logger.log(f"Exception occurred while connecting: {e}", flag=1, name=method_name)
                raise e
        return df
    except Exception as e:
        logger.log(f"Exception occurred while querying: {e}", flag=1, name=method_name)
        raise e

def get_model_from_table(table_name: str):
    # Map table names to ORM models
    table_model_map = {
        'job_information': JobInformation,
        'industry_relation': IndustryRelation,
        'industry': Industry,
        'dev_stack': DevStack,
        'job_stack': JobStack,
        'category': Category,
        'include_cartegory': IncludeCategory
    }
    if table_name in table_model_map:
        return table_model_map[table_name]
    else:
        raise ValueError(f"Table {table_name} not found in model map.")