import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from influxdb_client import InfluxDBClient
import plotly.graph_objects as go

import os
from dotenv import load_dotenv
load_dotenv()

INFLUXDB_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_ORG = "individual"
INFLUXDB_BUCKET = "anomaly-detection"

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

influxdb_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, timeout=30_000)
# we'll submit to this query api from influxdb to dash

query_api = influxdb_client.query_api() 

def query_influxdb_data():

    flux_query = f'''
    from(bucket : "{INFLUXDB_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r["_measurement"] == "sensor_readings")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''

    try:
        df = query_api.query_data_frame(query = flux_query)
        if df is not None and not df.empty:
            df = df.rename(columns={'_time': 'timestamp'})
            # Convert timestamp to a more readable format if needed (optional)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
    except Exception as e:
        print(f"Error querying InfluxDB: {e}")
        # Return an empty DataFrame in case of an error
        return pd.DataFrame()

app.layout = dbc.Container([
    # First row: The main title of the dashboard
    dbc.Row([
        dbc.Col(html.H1("Real-time Anomaly Detection Dashboard", className="text-center text-primary mb-4"), width=12)
    ]),

    dbc.Row([
        dbc.Col(
            # We use dbc.Card for a nice container
            dbc.Card(
                dbc.CardBody([
                    html.H6("Total Anomalies Detected", className="card-title text-center"),
                    # This H2 element will display our KPI.
                    # CRUCIAL: We give it a unique ID to target with our callback.
                    html.H2(id='anomaly-count-kpi', children="0", className="card-text text-center text-danger")
                ])
            ),
            width={"size": 4, "offset": 4} # Center a medium-width card
        )
    ], className="mb-4"), # Add some margin below the KPI row

    # Third row: A placeholder for our main graph (to be added in a later task)
    dbc.Row([
        dbc.Col([
            # The dcc.Graph component is the canvas for our visualizations.
            # We give it a unique ID so we can target it with callbacks.
            dcc.Graph(id='live-graph'),
            
            # The dcc.Interval component is the "heartbeat" of our app.
            # It triggers an update every 'interval' milliseconds.
            # It is not visible on the page itself.
            dcc.Interval(
                id='interval-component',
                interval=1*1000,  # in milliseconds (e.g., 1*1000 = 1 second)
                n_intervals=0     # number of times the interval has fired
            )
        ], width=12)
    ]),],
    fluid=True)

@app.callback(
    [Output('live-graph', 'figure'),
     Output('anomaly-count-kpi', 'children')]
    [Input('interval-component', 'n_intervals')]
)

def update_graph_and_kpi(n):
    df = query_influxdb_data()
    if df.empty:
        empty_fig = go.Figure(layout={
            'title': 'Waiting for data...',
            'xaxis': {'title': 'Timestamp'},
            'yaxis': {'title': 'Sensor Value'},
            'template': 'plotly_dark'
        })
        # The function must return a value for EACH output.
        return empty_fig, "0" 
    
    normal_data = df[df['is_anomaly' == False]]
    anomalies = df[df['is_anomaly' == True]]

    anomaly_count = len(anomalies)

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=normal_data['timestamp'], 
        y=normal_data['value'],
        mode='lines+markers',
        name='Normal', # This name will appear in the legend
        marker=dict(color='#1f77b4', size=5), # A standard blue color
        line=dict(width=2)
    ))

    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies['timestamp'], 
            y=anomalies['value'],
            mode='markers',
            name='Anomaly', # This will also appear in the legend
            marker=dict(
                color='red',        # A prominent color
                size=12,            # Larger size to stand out
                symbol='x',         # A distinct 'x' symbol
                line=dict(width=2)  # Thicker marker lines
            )
        ))

    fig.update_layout(
        df,
        x = 'timestamp',
        y = 'value',
        title = 'Live Sensor Readings'
        legend_title='Data Type',
        hovermode='x unified',
        template='plotly_dark'
    )
    # fig.update_layout(template='plotly_dark')
    # fig.update_traces(mode='lines+markers')

    return fig , str(anomaly_count)

if __name__ == "__main__":
    # app.run_server is the command that starts the Dash development web server.
    app.run(
        host='0.0.0.0', # This is CRUCIAL for Docker. It makes the server accessible from outside the container.
        port=8050,      # The port the web server will listen on.
        debug=True      # Enables hot-reloading: the app automatically updates when we save changes.
    )