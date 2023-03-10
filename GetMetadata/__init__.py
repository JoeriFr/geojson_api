import logging
import psycopg2
import json
import os

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("GetMetadata triggered")

    coord_system = req.params.get("coord_system")
    if not coord_system:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            coord_system = req_body.get("coord_system")

    if coord_system in ["4326", "28992"]:
        espg_code = coord_system
    else: 
        espg_code = "28992"

    conn = psycopg2.connect(
        host=os.getenv(""),
        database=os.getenv(""),
        user=os.getenv(""),
        password=os.getenv(""),
    )
    cur = conn.cursor()
    cur.execute(
        f"SELECT sensor_id, sensor_name, sensor_description, datastream_isactive, location_woonplaats,\
                location_description, location_huisnummer, location_postcode, ST_X(ST_TRANSFORM(geom, {espg_code})), ST_Y(ST_TRANSFORM(geom, {espg_code})) \
                FROM public.breda_laser_metadata_prod"
    )
    sensors = cur.fetchall()

    features = []

    for sensor in sensors:
        (
            sensor_id,
            sensor_name,
            sensor_description,
            datastream_isactive,
            location_woonplaats,
            location_description,
            location_huisnummer,
            location_postcode,
            x_rdc,
            y_rdc,
        ) = sensor

        geo_json_dict = {
            "type": "Feature",
            "properties": {
                "sensor_id": sensor_id,
                "sensor_name": sensor_name,
                "sensor_description": sensor_description,
                "datastream_isactive": datastream_isactive,
                "location_woonplaats": location_woonplaats,
                "location_description": location_description,
                "location_huisnummer": location_huisnummer,
                "location_postcode": location_postcode,
            },
            "geometry": {"type": "Point", "coordinates": [y_rdc, x_rdc]},
        }
        features.append(geo_json_dict)

    geo_json_collection = {
        "type": "FeatureCollection", 
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:EPSG::28992" 
            }
        },
        "features": features
    }
    
    return func.HttpResponse(json.dumps(geo_json_collection), status_code=200)
