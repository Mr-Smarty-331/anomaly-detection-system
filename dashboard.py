import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    # First row: The main title of the dashboard
    dbc.Row([
        dbc.Col(html.H1("Real-time Anomaly Detection Dashboard", className="text-center text-primary mb-4"), width=12)
    ]),

    # Second row: A placeholder for our main graph (to be added in a later task)
    dbc.Row([dbc.Col([
            html.H3("Live Sensor Readings"),html.P("This graph will display live data from InfluxDB.")],
            width=12)]),], fluid=True)



if __name__ == "__main__":
    # app.run_server is the command that starts the Dash development web server.
    app.run_server(
        host='0.0.0.0', # This is CRUCIAL for Docker. It makes the server accessible from outside the container.
        port=8050,      # The port the web server will listen on.
        debug=True      # Enables hot-reloading: the app automatically updates when we save changes.
    )