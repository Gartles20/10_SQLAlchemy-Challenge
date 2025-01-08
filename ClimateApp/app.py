import pandas as pd
from flask import Flask, jsonify
from SQL_Helper import SQLHelper


#################################################
# Flask Setup
#################################################
app = Flask(__name__)
sqlHelper = SQLHelper()


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"<a href='/api/v1.0/temperature' target='_blank'>/api/v1.0/temperature</a><br/>"
        f"<a href='/api/v1.0/2017-01-01' target='_blank'>/api/v1.0/2017-01-01</a><br/>"
        f"<a href='/api/v1.0/2017-01-01/2017-01-31' target='_blank'>/api/v1.0/2017-01-01/2017-01-31</a><br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Execute queries
    df = sqlHelper.queryPrecip()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)


@app.route("/api/v1.0/stations")
def stations():
    # Execute Query
    df = sqlHelper.queryStation()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)



@app.route("/api/v1.0/temperature")
def temperature2():
    # Execute Query
    df = sqlHelper.queryTemp()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/<start>")
def tstats1(start):
    # Execute Query
    df = sqlHelper.queryTStats(start)

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)


@app.route("/api/v1.0/<start>/<end>")
def tstats_startend1(start, end):
    # Execute Query
    df = sqlHelper.queryTStats_StartEnd(start, end)

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
