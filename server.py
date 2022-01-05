from datetime import datetime
import json
import os
from pathlib import Path
import shutil

from flask import Flask, send_file
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
import werkzeug
import pandas as pd
from openpyxl import load_workbook

import sys

import boto3
from botocore.exceptions import ClientError

from dataframe_utils import load_excel, remap_columns, save_as_excel

app = Flask(__name__)
api = Api(app)
CORS(app)

s3_client = boto3.client('s3')

# FILES_ROOT = Path(os.getenv("FILES_ROOT"))
# FILES_ROOT = Path(__file__).parent  # v.local
FILES_ROOT = Path("/tmp") # v.for lambda temp folder

CONFIG_PATH = Path(__file__).parent / "config.json"  #  v.local
with open(CONFIG_PATH, "r") as fp:
    CONFIG = json.load(fp)

bucket = os.environ['TARGET_BUCKET'] if os.environ.get('LAMBDA_TASK_ROOT') is not None else "mycanopybucket"

@app.route("/")
def hello():
    return "Canopy: Ad Astra Per Aspera! "+ ("We are offline" if os.environ.get('LAMBDA_TASK_ROOT') is None else "We are online")


class UploadFile(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        "file",
        type=werkzeug.datastructures.FileStorage, 
        location='files'
    )
    def post(self):
        app.logger.debug("uploading file")

        args = self.parser.parse_args()
        file = args["file"]
        file.save(FILES_ROOT / "input.xlsx")
        print("Is input in tmp? " + str(os.path.isfile('/tmp/input.xlsx')), file=sys.stderr)
        # app.logger.error(print(os.path.isfile('/tmp/input.xlsx'))
       
        """
        Adding the S3 Upload
        """
        try:
            s3_client.upload_file(str(FILES_ROOT)+"/input.xlsx", bucket, "input.xlsx")
        except ClientError as error:
            raise error
        return self._sheetnames(file)


    def get(self):
        """
        Get the list of available sheet names
        """
        app.logger.debug("asking for sheet names")
        file_path = FILES_ROOT / "input.xlsx"
        if file_path.exists():
            return self._sheetnames(file_path)
        else:
            try:
              s3_client.download_file(bucket, "input.xlsx", str(FILES_ROOT)+ "/input.xlsx")
              return self._sheetnames(file_path)
            except ClientError as error:
               print("No input file retrieved from S3", file=sys.stderr)
               return self._sheetnames()
            

    def _sheetnames(self, file=None):
        if file is not None:
            excel_file = pd.ExcelFile(file)
            sheet_names = list(excel_file.sheet_names)
        else:
            sheet_names = []
        return {"sheet_names": sheet_names}


api.add_resource(UploadFile, "/upload")


class ProcessExcel(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        "sheet_name",
        type=str,
    )

    def get(self):
        file_path = FILES_ROOT / "inter.feather"
        if file_path.exists():
            dataframe = pd.read_feather(file_path)
            return self._header(dataframe)
        else:
            try:
              s3_client.download_file(bucket, "input.xlsx", str(FILES_ROOT)+ "/input.xlsx")
              dataframe = pd.read_feather(file_path)
              return self._header(dataframe)
            except ClientError as error:
                print("No input file retrieved from S3", file=sys.stderr)
                return self._header()        

    def post(self):
        """
        Read given excel sheet and process it. Return the resulting header
        """
        args = self.parser.parse_args()
        sheet_name = args["sheet_name"]
        dataframe = load_excel(
            FILES_ROOT / "input.xlsx", sheet_name=sheet_name
        ).astype(object)

        notnull_mask = dataframe.notnull()
        dataframe[notnull_mask] = dataframe[notnull_mask].astype(str)
        dataframe.to_feather(FILES_ROOT / "inter.feather")

        if (FILES_ROOT / "db.json").exists():
            with open(FILES_ROOT / "db.json", "r") as fp:
                db = json.load(fp)
            saved_propositions = db.get("saved_propositions", {})
        else:
            saved_propositions = {}

        mapping = {
            col: None for col in dataframe.columns
        }
        for (source, target) in saved_propositions.items():
            candidates = {
                key for key in CONFIG if key not in mapping.values()
            }
            if source in mapping and target in candidates:
                mapping[source] = target

        with open(FILES_ROOT / "db.json", "w") as fp:
            json.dump(
                {
                    "mapping": mapping,
                    "reference_date": None,
                    "saved_propositions": saved_propositions
                },
                fp
            )

        return self._header(dataframe)

    def _header(self, df=None):
        if df is not None:
            header = list(df.columns)
        else:
            header = None
        return {"header" : header}


api.add_resource(ProcessExcel, "/process")


@app.route("/download")
def download_file():
    file_path = FILES_ROOT / "output.xlsx"
    return send_file(file_path, as_attachment=True)


class MappingResource(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument("mapping", type=dict)
    parser.add_argument(
        "reference_date",
        required=False,
        nullable=True,
        type=datetime
    )

    def get(self):
        app.logger.debug("Reading current mapping")
        with open(FILES_ROOT / "db.json", "r") as fp:
            db = json.load(fp)
        return {
            "mapping": db["mapping"],
            "reference_date": db["reference_date"],
            "config": CONFIG
        }

    def _update_mapping(self, old_mapping, new_mapping):
        new_targets = set(new_mapping.values())
        return {
            **{
                key: value if value not in new_targets else None
                for (key, value) in old_mapping.items()
            },
            **new_mapping
        }

    def patch(self):
        app.logger.debug("Patching current mapping")        
        args = self.parser.parse_args()
        mapping = args["mapping"]
        reference_date = args["reference_date"]

        with open(FILES_ROOT / "db.json", "r") as fp:
            db = json.load(fp)
        new_mapping = self._update_mapping(db.get("mapping", {}), mapping)

        new_db = {
            **db,
            "mapping": new_mapping,
            "reference_date": reference_date
        }

        with open(FILES_ROOT / "db.json", "w") as fp:
            json.dump(new_db, fp)
        return {
            "mapping": new_db["mapping"], 
            "reference_date": new_db["reference_date"]
        }

    def post(self):
        app.logger.debug("validating current mapping")
        df = pd.read_feather(FILES_ROOT / "inter.feather")

        with open(FILES_ROOT / "db.json", "r") as fp:
            db = json.load(fp)
        mapping = db["mapping"]
        columns = {
            source: CONFIG[target]["name"]
            for (source, target) in mapping.items()
            if target is not None
        }
        output_df = remap_columns(df, columns, drop_unmapped=True)

        template_path = Path(__file__).parent / "output_template.xlsx"
        if template_path.exists():
            shutil.copyfile(
                Path(__file__).parent / "output_template.xlsx",
                FILES_ROOT / "output.xlsx"
            )
        else:
            if (FILES_ROOT / "output.xlsx").exists():
                (FILES_ROOT / "output.xlsx").unlink()
        save_as_excel(
            output_df, FILES_ROOT / "output.xlsx", sheet_name="input print",
            startrow=0
        )

        new_db = {
            **db,
            "saved_propositions": {
                **db.get("saved_propositions", {}), **mapping
            }
        }

        with open(FILES_ROOT / "db.json", "w") as fp:
            json.dump(new_db, fp)


        return {}


api.add_resource(MappingResource, "/mapping")


if __name__ == '__main__':
    HOST = os.getenv("HOST")
    app.logger.debug(f"Starting Server with host: {HOST}")
    app.run(debug=True, host=HOST)