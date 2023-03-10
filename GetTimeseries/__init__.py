import logging
import psycopg2
import json
import os
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    sensor_id = req.params.get("sensor_id")
    if not sensor_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            sensor_id = req_body.get("sensor_id")

    if sensor_id:
        conn = psycopg2.connect(
            host=os.getenv(""),
            database=os.getenv(""),
            user=os.getenv(""),
            password=os.getenv(""),
        )
        cur = conn.cursor()
        cur.execute(
            f"SELECT datetime::TEXT, breda_laser_prod.in, out FROM public.breda_laser_prod WHERE sensor_id='{sensor_id}'"
        )
        timeseries = cur.fetchall()
        if not timeseries:
            return func.HttpResponse(f"No sensor found with ID: '{sensor_id}'")
        series_dict = {
            "sensor_id": sensor_id,
            "timeseries_format": ("datetime", "in", "out"),
            "timeseries": timeseries
        }
        return func.HttpResponse(json.dumps(series_dict), status_code=200)
    else:
        return func.HttpResponse(
            "No sensor ID given.",
            status_code=200,
        )