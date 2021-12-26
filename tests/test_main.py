# -*- encoding:utf-8 -*-
import os
import json
from src import create_file
from datetime import datetime, timedelta, timezone
from decimal import Decimal


def get_absolute_path(path: str) -> str:
    return os.path.abspath(path)


def absolute_path_from(joiner: str) -> str:
    return os.path.join(
        os.path.dirname(
            get_absolute_path(__file__)
        ),
        joiner
    )


os.makedirs(f"{os.path.dirname(get_absolute_path(__file__))}/target", exist_ok=True)


def test_json_serialization():
    UTC = timezone(timedelta(hours=0, minutes=0))
    now = datetime.now(UTC)
    expected = json.dumps({
        "a": "string",
        "b": 10,
        "c": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "d": 1.1111111121
    })
    assert expected, json.dumps({
        "a": "string",
        "b": 10,
        "c": now,
        "d": Decimal('1.1111111121')
    }, default=create_file.json_serialization)
    # with self.assertRaises(TypeError,
    #                         msg=f"Invalid object. cannot serialize obj = {unittest.TestCase} , type = {type(unittest.TestCase)}"):
    #     json.dumps({
    #         "dummy": unittest.TestCase,
    #     }, default=create_file.json_serialization)


def test_csv_to_json():
    expected = {
        "0": {
            "0": "34",
            "1": "Ba"
        },
        "test": {
            "0": "G",
            "1": "43"
        },
        "V": {
            "0": "SD",
            "1": "Ω"
        },
        "123c": {
            "0": 432,
            "1": 421
        },
        "345.6": {
            "0": "B",
            "1": "α"
        }
    }
    create_file.csv_to_json(absolute_path_from("test_data/csv_to_json.csv"),
                            absolute_path_from("target/csv_to_json.json"))
    with open(absolute_path_from("target/csv_to_json.json")) as fp:
        assert expected == json.load(fp)


def test_csv_to_orc():
    expected = "xSA1,vVSxxSx9,123.4,123c,48515\nBFΩ,GsSs1.43,120.0,XX,991\nαASAx,asdASSxx,489.0068,CXX,782"
    create_file.csv_to_orc(absolute_path_from("test_data/csv_to_orc.csv"),
                           absolute_path_from("target/csv_to_orc.orc"))
    create_file.orc_to_csv(absolute_path_from("target/csv_to_orc.orc"), absolute_path_from("target/csv_to_orc.csv"))
    with open(absolute_path_from("target/csv_to_orc.csv")) as fp:
        assert expected == fp.read()


def test_csv_to_parquet():
    expected = "Xc,92,7R7,123.63,XCQ\nBf,432,SxD,432.67,CS\nΩ/,100,15G,994.004,αxX\n"
    create_file.csv_to_parquet(
        absolute_path_from("test_data/csv_to_parquet.csv"),
        absolute_path_from("target/csv_to_parquet.pq"),
        {
            "header": None,
            "names": ["col1", "col2", "col3", "col4", "col5"]
        }
    )
    create_file.parquet_to_csv(absolute_path_from("target/csv_to_parquet.pq"),
                               absolute_path_from("target/csv_to_parquet.csv"))
    with open(absolute_path_from("target/csv_to_parquet.csv")) as fp:
        assert expected, fp.read()


def test_excel_to_csv():
    expected = "1,test,Ω\n2,test2,Δ\nAAA,α,432\n"
    create_file.excel_to_csv(
        absolute_path_from("test_data/excel_to_csv.xlsx"),
        "Sheet1",
        absolute_path_from("target/excel_to_csv.csv")
    )
    with open(absolute_path_from("target/excel_to_csv.csv")) as fp:
        assert expected, fp.read()


def test_excel_to_json():
    expected = {
        "0": {
            "0": 1,
            "1": 2,
            "2": "AAA"
        },
        "1": {
            "0": "test",
            "1": "test2",
            "2": "α"
        },
        "2": {
            "0": "Ω",
            "1": "Δ",
            "2": 432
        }
    }
    create_file.excel_to_json(
        absolute_path_from("test_data/excel_to_json.xlsx"),
        "Sheet1",
        absolute_path_from("target/excel_to_json.json")
    )
    with open(absolute_path_from("target/excel_to_json.json")) as fp:
        assert expected, json.load(fp)


def test_json_to_csv():
    expected = "1,a,ax,as@,a\n2,x,,False,c\n3,Ω,a,aas,12.423\n"
    create_file.json_to_csv(absolute_path_from("test_data/json_to_csv.json"),
                            absolute_path_from("target/json_to_csv.csv"))
    with open(absolute_path_from("target/json_to_csv.csv")) as fp:
        assert expected, fp.read()


def test_json_to_excel():
    expected = {
        "0": {
            "0": 1,
            "1": 2,
            "2": 3
        },
        "1": {
            "0": "a",
            "1": "x",
            "2": "Ω"
        },
        "2": {
            "0": "ax",
            "1": None,
            "2": "a"
        },
        "3": {
            "0": "as@",
            "1": False,
            "2": "aas"
        },
        "4": {
            "0": "a",
            "1": "c",
            "2": 12.423
        }
    }
    create_file.json_to_excel(absolute_path_from("test_data/json_to_excel.json"),
                              absolute_path_from("target/json_to_excel.xlsx"))
    create_file.excel_to_json(absolute_path_from("target/json_to_excel.xlsx"),
                              "Sheet1", absolute_path_from("target/json_to_excel.json"))
    with open(absolute_path_from("target/json_to_excel.json")) as fp:
        assert expected == json.load(fp)
