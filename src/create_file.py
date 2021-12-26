# -*- encoding:utf-8 -*-
from datetime import datetime
from decimal import Decimal
import json
from logging import getLogger, StreamHandler, DEBUG
import os
import re
# Third party
import pandas as pd
import pyorc


# logger setting
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(os.getenv("LogLevel", DEBUG))
logger.addHandler(handler)
logger.propagate = False


def json_serialization(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S.%f")
    elif isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Invalid object. cannot serialize obj = {obj} , type = {type(obj)}")


def csv_to_json(source_path: str, target_path: str) -> None:
    """write something into a json file based on a csv file"""

    df = pd.read_csv(source_path)
    columns = df.columns

    for row in df.itertuples(index=False):
        item = {}
        for i, column in enumerate(row):
            item[columns[i]] = column

    for _, item in df.iteritems():
        pass

    df.to_json(target_path, force_ascii=False, indent=4)
    # with open(target_path,"w") as fp:
    #     json.dump(data,fp,indent=4,ensure_ascii=False)


def csv_to_orc(source_path: str, target_path: str) -> None:
    """write something into a orc file based on a csv file"""

    with open(target_path, "wb") as data:

        # Read source data. In this case, We'll convert CSV to ORC
        with open(source_path, "r") as source:
            # Get rid of \n "return code"
            lines = [i.strip() for i in source.readlines()]
            records = []
            header_name = []
            # rows process
            for line in lines:
                record = []
                # colums process
                for column in line.split(","):
                    # Data process
                    if re.match(r'^\d+\.\d+$', column):
                        record.append(float(column))
                        # header process
                        if line == lines[0]:
                            header_name.append("double")
                    elif re.match(r'^\d+$', column):
                        record.append(int(column))
                        # header process
                        if line == lines[0]:
                            header_name.append("int")
                    else:
                        record.append(column.strip('"') if re.match(r'^".*"$', column) else column)
                        # header process
                        if line == lines[0]:
                            header_name.append("string")

                # one record is packed as a tuple
                records.append(tuple(record))

                # If we are at the first record, we'll give the column names to the ORC table
                if line == lines[0]:
                    for i in range(len(header_name)):
                        header_name[i] = f"col{i}:{header_name[i]}"
                    header_name = f'struct<{",".join(header_name)}>'

            # Get writer Object. give ORC file object at the position of first augument,
            # column names at the position of second augument  "Writer" method
            with pyorc.Writer(data, header_name) as writer:
                for record in records:
                    writer.write(record)


def csv_to_parquet(source_path: str, target_path: str, config={}) -> None:
    """write something into a parquet file based on a csv file"""

    df = pd.read_csv(source_path, **config)
    df.to_parquet(target_path)

    # arrow_table = pyarrow.csv.read_csv(source_path)
    # pyarrow.parquet.write_table(arrow_table, target_path)


def excel_to_csv(source_path: str, source_sheet: str, target_path: str) -> None:
    """write something into a csv file based on a excel file"""

    df = pd.read_excel(source_path, sheet_name=source_sheet,
                       header=None, index_col=None, usecols=None, skiprows=None).iloc[:, :]
    columns = df.columns

    for row in df.itertuples(index=False):
        item = {}
        for i, column in enumerate(row):
            item[columns[i]] = column

    for _, item in df.iteritems():
        pass

    df.to_csv(target_path, index=False, header=None)


def excel_to_json(source_path: str, source_sheet: str, target_path: str) -> None:
    """write something into a json file based on a excel file"""

    df = pd.read_excel(source_path, sheet_name=source_sheet,
                       header=None, index_col=None, usecols=None, skiprows=None).iloc[:, :]
    columns = df.columns

    for row in df.itertuples(index=False):
        item = {}
        for i, column in enumerate(row):
            item[columns[i]] = column

    for _, item in df.iteritems():
        pass

    df.to_json(target_path, force_ascii=False, indent=4)
    # with open(target_path,"w") as fp:
    #     json.dump(data,fp,indent=4,ensure_ascii=False)


def json_to_csv(source_path: str, target_path: str) -> None:
    """write something into a csv file based on a json file"""

    with open(source_path, "r") as fp:
        df = pd.DataFrame(json.load(fp))
    columns = df.columns

    for row in df.itertuples(index=False):
        item = {}
        for i, column in enumerate(row):
            item[columns[i]] = column

    for _, item in df.iteritems():
        pass

    df.to_csv(target_path, index=False, header=None)


def json_to_excel(source_path: str, target_path: str) -> None:
    """write something into a excel file based on a json file"""

    with open(source_path, "r") as fp:
        df = pd.DataFrame(json.load(fp))
    columns = df.columns

    for row in df.itertuples(index=False):
        item = {}
        for i, column in enumerate(row):
            item[columns[i]] = column

    for _, item in df.iteritems():
        pass

    df.to_excel(target_path, index=False, header=False)


def parquet_to_csv(source_path: str, target_path: str) -> None:
    """write something into a csv file based on parquet file"""

    df = pd.read_parquet(source_path)
    df.to_csv(target_path, index=False, header=False)

    # arrow_table = pyarrow.parquet.read_table(source_path)
    # df = arrow_table.to_pandas()
    # df.to_csv(target_path)


def orc_to_csv(source_path: str, target_path: str) -> None:
    """write something into a csv file based on parquet file"""

    with open(source_path, "rb") as data:
        # Get Reader class object from ORC file without column names
        reader = pyorc.Reader(data)
        # Get just only column names from ORC file
        # columns = reader.schema.fields

        # Get each column name
        # for column in columns:
        #     logger.debug(column)
        #     logger.debug(columns[column].kind)

        with open(target_path, "w") as fp:
            # loop row Reader class object
            records = []
            for one_record_data in reader:
                records.append(','.join(map(str, one_record_data)))
            fp.write("\n".join(records))
