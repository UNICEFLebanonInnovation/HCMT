import os
import datetime

from flask import Flask, redirect

from mongoengine import *

import flask_admin as admin
from flask_admin import BaseView, expose
from flask_admin.contrib.mongoengine.filters import FilterEqual
from flask_mongoengine import MongoEngine
from flask_admin.form import rules
from flask_admin.contrib.mongoengine import ModelView, EmbeddedForm

# Create application
app = Flask(__name__)

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'
app.config['MONGODB_SETTINGS'] = {
    'db': 'wash',
    'host': os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/wash'),
}

# Create models
db = MongoEngine()
db.init_app(app)


# Define mongoengine documents
class KoboData(DynamicEmbeddedDocument):

    safe_water = BooleanField(db_field='Safe Water')
    hands_washed = BooleanField(db_field='Hands Washed')
    clean_latrines = BooleanField(db_field='Clean Latrines')
    disease_prevention = BooleanField(db_field='Disease Prevention')
    diseases = StringField(db_field='Diseases')

    household_data = DictField(db_field='Metadata_section/household_data')


class SiteDetailsMixin(object):
    p_code_internal_code = StringField(db_field='p_code')
    p_code_name = StringField()
    date = StringField()
    visit = StringField()
    governorate = StringField()
    partner = StringField()
    by = StringField(db_field='org')

    individuals = IntField(db_field='indivisuals')
    total_tents = IntField()
    min_sample_tents = IntField(db_field='sample_tents_min')
    actual_tents_sampled = IntField(db_field='sample_tents')

    free_of_open_defecation = StringField(db_field='Free of Open Defecation')
    clean_environment = StringField(db_field='Clean Environment')
    no_solid_waste = StringField(db_field='No Waste')

    safe_water = DecimalField(db_field='Safe Water')
    hands_washed = DecimalField(db_field='Hands Washed')
    clean_latrines = DecimalField(db_field='Clean Latrines')
    locked_latrines = DecimalField(db_field='Locked Latrines')
    lighted_latrines = DecimalField(db_field='Lighted Latrines')
    disease_prevention = DecimalField(db_field='Disease Prevention')
    diseases = ListField(StringField(), db_field='Diseases')

    household_score = DecimalField(db_field='Household Score')
    site_score = DecimalField(db_field='Site Score')
    total_score = DecimalField(db_field='Total Score')

    gbv_number = IntField(db_field='GBV_total')
    gbv_number_female = IntField(db_field='GBV_total_female')
    gbv_number_male = IntField(db_field='GBV_total_male')
    gbv_number_female_negative = IntField(db_field='GBV_total_female_negative')
    gbv_number_male_negative = IntField(db_field='GBV_total_male_negative')
    gbv_score_percentage = IntField(db_field='GBV')
    gbv_score_percentage_female = IntField(db_field='GBV_female')
    gbv_score_percentage_male = IntField(db_field='GBV_male')

    report = StringField(db_field='Report on AI')


class Visits(SiteDetailsMixin, DynamicEmbeddedDocument):
    pass


class Sites(SiteDetailsMixin, DynamicDocument):

    visits = ListField(EmbeddedDocumentField(Visits))


# Customized admin views
class SiteView(ModelView):
    can_create = False
    can_delete = False
    can_export = True
    can_edit = False
    can_view_details = True
    page_size = 50

    column_list = (
        'p_code_internal_code',
        'p_code_name',
        'date',
        'visit',
        'by',
        'partner',
        'governorate',
        'total_tents',
        'individuals',
        'min_sample_tents',
        'free_of_open_defecation',
        'clean_environment',
        'no_solid_waste',
        'site_score',
        'safe_water',
        'hands_washed',
        'clean_latrines',
        'locked_latrines',
        'lighted_latrines',
        'diseases',
        'disease_prevention',
        'household_score',
        'total_score',
        'gbv_number',
        'gbv_number_female',
        'gbv_number_male',
        'gbv_number_female_negative',
        'gbv_number_male_negative',
        'gbv_score_percentage',
        'gbv_score_percentage_female',
        'gbv_score_percentage_male',
        'report',
    )

    column_filters = (
        'p_code_internal_code',
        'p_code_name',
        'visit',
        'by',
        'total_tents',
        'individuals',
        'min_sample_tents',
        'free_of_open_defecation',
        'clean_environment',
        'no_solid_waste',
        'safe_water',
        'hands_washed',
        'clean_latrines',
        'locked_latrines',
        'lighted_latrines',
        'disease_prevention',
        'site_score',
        'household_score',
        'total_score',
        'gbv_number',
        'gbv_number_female',
        'gbv_number_male',
        'gbv_number_female_negative',
        'gbv_number_male_negative',
        'gbv_score_percentage',
        'gbv_score_percentage_female',
        'gbv_score_percentage_male',
        'report',
        FilterEqual(
            column=Sites.partner,
            name='Partner',
            options=[(partner,partner) for partner in Sites.objects.distinct('partner')]
        ),
        FilterEqual(
            column=Sites.governorate,
            name='Governorate',
            options=[(governorate, governorate) for governorate in Sites.objects.distinct('governorate')]
        ),
    )

    column_searchable_list = (
        'p_code_internal_code',
        'p_code_name',
        'date',
        'visit',
        'by',
        'partner',
        'governorate',
    )

    form_subdocuments = {
        'visits': {
            'form_subdocuments': {
                None: {
                    # Add <hr> at the end of the form
                    'form_columns': (
                        'date',
                        'visit',
                        'partner',
                        'governorate',
                        'by',
                        'individuals',
                        'total_tents',
                        'min_sample_tents',
                        'actual_tents_sampled',
                        'free_of_open_defecation',
                        'clean_environment',
                        'no_solid_waste',
                        'safe_water',
                        'hands_washed',
                        'clean_latrines',
                        'locked_latrines',
                        'lighted_latrines',
                        'disease_prevention',
                        'total_score',
                        'diseases',
                        #'raw_data',
                        #rules.HTML('<hr>')
                    ),
                    'form_widget_args': {
                        'total_score': {
                            'style': 'color: red'
                        }
                    }
                }
            }
        }
    }


# Create admin
admin = admin.Admin(app, 'Healthy Camp Monitoring Tool')

# Add views
admin.add_view(SiteView(Sites))


# Flask views
@app.route('/')
def index():
    return redirect('/admin')


if __name__ == '__main__':
    # Start app
    app.run(debug=True, use_reloader=True)