import json
import bson
import click
from os import path

import requests
from requests.auth import HTTPBasicAuth

from app import app, db


def _get_aggreagation(name):
    with open('group_by_site.json') as json_data:
        return json.load(json_data)


@click.argument('password')
@click.argument('username')
@click.argument('kobo_id')
@app.cli.command()
def import_form_data(
        url='https://kc.humanitarianresponse.info/api/v1/data',
        kobo_id='',
        username='',
        password='',
        collection='submissions'
):
    """Initialize the database."""
    click.echo('Staring import from Kobo...')

    # url = path.join(url, kobo_id)
    url = path.join(url, kobo_id+'?query={"_submission_time": {"$gt": "2018-12-30T10:42:01"}}')

    data = requests.get(
        url,
        auth=HTTPBasicAuth(username, password))

    db.connection.get_default_database()[collection].drop()
    db.connection.get_default_database()[collection].insert_many(data.json())
    click.echo('{} submissions imported from Kobo'.format(len(data.json())))


@app.cli.command()
def convert_to_integer(
        collection='submissions',
        field_name='Metadata_section/site_data/sample_tents'
):
    for form in db.connection.get_default_database()[collection].find():
        if field_name not in form:
            continue
        form[field_name] = int(form[field_name])
        db.connection.get_default_database()[collection].save(form)


@app.cli.command()
def run_aggregation(
        collection='submissions',
        file_name='group_by_site'
):
    click.echo(db.connection.get_default_database().command(
        'aggregate',
        collection,
        pipeline=_get_aggreagation(file_name),
        allowDiskUse=True,
        cursor={},
        # explain=True
    ))
