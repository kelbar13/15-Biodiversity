import datetime as dt
import numpy as np
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, desc

from flask import (
    Flask,
    render_template,
    jsonify,
    request,
    redirect)

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///DataSets/belly_button_biodiversity.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
Base.classes.keys()

# Save references to the table
Samples = Base.classes.samples
OTU = Base.classes.otu
Samples_Metadata = Base.classes.samples_metadata

# Create session
session = Session(engine)



#################################################
# Flask Setup and Routes
#################################################

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')

# List of Sample Names
@app.route('/names')
def names():
    samples_stmt = session.query(Samples).statement
    samples_df = pd.read_sql_query(samples_stmt, session.bind)
    samples_df.set_index("otu_id", inplace = True)
    return jsonify(list(samples_df.columns))

# List of OTU Descriptions 
@app.route('/otu')
def otu_descriptions():
    otu_stmt = session.query(OTU).statement
    otu_df = pd.read_sql_query(otu_stmt, session.bind)
    otu_df.set_index("otu_id", inplace=True)
    return jsonify(list(otu_df.columns))

# MetaData for a given sample
@app.route('/metadata/<sample>')
def metadata(sample):
    sel = [Samples_Metadata.SAMPLEID, Samples_Metadata.ETHNICITY,
           Samples_Metadata.GENDER, Samples_Metadata.AGE,
           Samples_Metadata.LOCATION, Samples_Metadata.BBTYPE]

    
    results = session.query(*sel).\
        filter(Samples_Metadata.SAMPLEID == sample[3:]).all()

    sample_metadata = {}
    for result in results:
        sample_metadata['SAMPLEID'] = result[0]
        sample_metadata['ETHNICITY'] = result[1]
        sample_metadata['GENDER'] = result[2]
        sample_metadata['AGE'] = result[3]
        sample_metadata['LOCATION'] = result[4]
        sample_metadata['BBTYPE'] = result[5]

    return jsonify(sample_metadata)

# Weekly Washing Frequency
@app.route('/wfreq/<sample>')
def wfreq(sample):
    sample_id = int(sample[3:])
    results = session.query(Samples_Metadata)
    for result in results:
        if sample_id == result.SAMPLEID:
            wfreq = result.WFREQ
    return jsonify(wfreq)

# OTU IDs and SAMPLE Values 
@app.route("/samples/<sample>")
def samples(sample):

    # Grab info
    stmt = session.query(Samples).statement
    df = pd.read_sql_query(stmt, session.bind)
    
    # Make sure sample was found in the columns
    if sample not in df.columns:
        return jsonify(f"Error Sample: {sample} not found!")
    
    # Return any sample values greater than 1
    df = df[df[sample] > 1]

    # Sort the results by sample in descending order
    df = df.sort_values(by=sample, ascending=0)

    # Format the data to send as json
    data = [{
        "otu_ids": df[sample].index.values.tolist(),
        "sample_values": df[sample].values.tolist()
    }]
    return jsonify(data)

# Initiate Flask app
if __name__ == "__main__":
    app.run(debug=True)

