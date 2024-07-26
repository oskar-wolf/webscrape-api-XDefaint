import os
import h5py
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc 
import logging
from dash_bootstrap_templates import load_figure_template
import plotly.express as px
from wordcloud import WordCloud
import base64
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def convert_bytes_to_str(data):
    """Convert bytes to string for all elements in the DataFrame."""
    return data.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)

# Load, create, and visualize finance data in one function
def visualize_finance_data(hdf5_path):
    try:
        finance_group = 'finance'
        with h5py.File(hdf5_path, 'r') as hdf:
            subgroups = list(hdf[finance_group].keys())
            logging.debug(f"Subgroups in 'finance': {subgroups}")
            latest_subgroup = subgroups[-1]
            logging.debug(f"Latest subgroup: {latest_subgroup}")
            dataset = hdf[f"{finance_group}/{latest_subgroup}/preprocessed_data"]
            data = pd.DataFrame(dataset[:], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            data = convert_bytes_to_str(data)
            logging.debug(f"Data loaded: {data.head()}")
        
        fig = go.Figure(data=[go.Candlestick(
            x=pd.to_datetime(data['timestamp']),
            open=data['open'].astype(float),
            high=data['high'].astype(float),
            low=data['low'].astype(float),
            close=data['close'].astype(float)
        )])
        fig.update_layout(
            title='Publisher Stock Price: Ubisoft Entertainment SA ',
            xaxis_title='Timestamp',
            yaxis_title='Price',
            xaxis_rangeslider_visible=False,
            template='superhero'  # Use Plotly's dark theme
        )
        return fig
    except Exception as e:
        logging.error(f"Failed to visualize finance data: {e}")
        return go.Figure()

def load_ign_ratings_data(hdf5_path):
    try:
        ign_group = 'ign_ratings'
        with h5py.File(hdf5_path, 'r') as hdf:
            subgroups = list(hdf[ign_group].keys())
            logging.debug(f"Subgroups in 'ign_ratings': {subgroups}")
            latest_subgroup = subgroups[-1]
            logging.debug(f"Latest subgroup: {latest_subgroup}")
            dataset = hdf[f"{ign_group}/{latest_subgroup}/preprocessed_data"]
            data = pd.DataFrame(dataset[:], columns=['timestamp', 'ign_rating', 'user_rating'])
            data = convert_bytes_to_str(data)
            logging.debug(f"IGN ratings data loaded: {data.head()}")
            latest_data = data.iloc[-1]
            return latest_data['ign_rating'], latest_data['user_rating']
    except Exception as e:
        logging.error(f"Failed to load IGN ratings data: {e}")
        return None, None

def load_player_counts_data(hdf5_path):
    try:
        player_counts_group = 'player_counts'
        with h5py.File(hdf5_path, 'r') as hdf:
            subgroups = list(hdf[player_counts_group].keys())
            logging.debug(f"Subgroups in 'player_counts': {subgroups}")
            latest_subgroup = subgroups[-1]
            logging.debug(f"Latest subgroup: {latest_subgroup}")
            dataset = hdf[f"{player_counts_group}/{latest_subgroup}/preprocessed_data"]
            data = pd.DataFrame(dataset[:], columns=['timestamp', 'player_count', 'percentage_change'])
            data = convert_bytes_to_str(data)
            logging.debug(f"Player counts data loaded: {data.head()}")
            latest_data = data.iloc[-1]
            return latest_data['player_count'], latest_data['percentage_change']
    except Exception as e:
        logging.error(f"Failed to load player counts data: {e}")
        return None, None

def load_search_data(hdf5_path, group_name):
    try:
        with h5py.File(hdf5_path, 'r') as hdf:
            subgroups = list(hdf[group_name].keys())
            logging.debug(f"Subgroups in '{group_name}': {subgroups}")
            latest_subgroup = subgroups[-1]
            logging.debug(f"Latest subgroup: {latest_subgroup}")
            dataset = hdf[f"{group_name}/{latest_subgroup}/preprocessed_data"]
            data = pd.DataFrame(dataset[:], columns=['count'])
            data = convert_bytes_to_str(data)
            logging.debug(f"{group_name} data loaded: {data.head()}")
            latest_data = data.iloc[-1]
            return latest_data['count']
    except Exception as e:
        logging.error(f"Failed to load {group_name} data: {e}")
        return None

def load_leaderboard_data(hdf5_path):
    try:
        leaderboard_group = 'leaderboards'
        with h5py.File(hdf5_path, 'r') as hdf:
            subgroups = list(hdf[leaderboard_group].keys())
            logging.debug(f"Subgroups in 'leaderboards': {subgroups}")
            latest_subgroup = subgroups[-1]
            logging.debug(f"Latest subgroup: {latest_subgroup}")
            dataset = hdf[f"{leaderboard_group}/{latest_subgroup}/preprocessed_data"]
            data = pd.DataFrame(dataset[:], columns=['rank', 'name', 'country', 'kills', 'matches_played'])
            data = convert_bytes_to_str(data)
            logging.debug(f"Leaderboard data loaded: {data.head()}")
            return data
    except Exception as e:
        logging.error(f"Failed to load leaderboard data: {e}")
        return pd.DataFrame()

def visualize_reddit_posts_data(hdf5_path):
    try:
        posts_group = 'reddit_XDefiant_posts'
        with h5py.File(hdf5_path, 'r') as hdf:
            subgroups = list(hdf[posts_group].keys())
            logging.debug(f"Subgroups in 'reddit_XDefiant_posts': {subgroups}")
            latest_subgroup = subgroups[-1]
            logging.debug(f"Latest subgroup: {latest_subgroup}")
            dataset = hdf[f"{posts_group}/{latest_subgroup}/preprocessed_data"]
            data = pd.DataFrame(dataset[:], columns=['title', 'textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score'])
            data = convert_bytes_to_str(data)
            logging.debug(f"Reddit posts data loaded: {data.head()}")

        # Convert numerical columns to float
        for column in ['textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score']:
            data[column] = pd.to_numeric(data[column], errors='coerce')
        
        # Drop rows with NaN values in numerical columns
        data = data.dropna(subset=['textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score'])

        reddit_posts_mean = data[['textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score']].mean()

        wordcloud = WordCloud(width=800, height=400, background_color='black').generate(' '.join(data['title']))
        wordcloud_image = BytesIO()
        wordcloud.to_image().save(wordcloud_image, format='PNG')
        wordcloud_image = base64.b64encode(wordcloud_image.getvalue()).decode('utf-8')

        return reddit_posts_mean, wordcloud_image
    except Exception as e:
        logging.error(f"Failed to visualize Reddit posts data: {e}")
        return pd.Series(), ""

def visualize_reddit_comments_data(hdf5_path):
    try:
        comments_group = 'reddit_XDefiant_comments'
        with h5py.File(hdf5_path, 'r') as hdf:
            subgroups = list(hdf[comments_group].keys())
            logging.debug(f"Subgroups in 'reddit_XDefiant_comments': {subgroups}")
            latest_subgroup = subgroups[-1]
            logging.debug(f"Latest subgroup: {latest_subgroup}")
            dataset = hdf[f"{comments_group}/{latest_subgroup}/preprocessed_data"]
            data = pd.DataFrame(dataset[:], columns=['body', 'textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score'])
            data = convert_bytes_to_str(data)
            logging.debug(f"Reddit comments data loaded: {data.head()}")

        # Convert numerical columns to float
        for column in ['textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score']:
            data[column] = pd.to_numeric(data[column], errors='coerce')
        
        # Drop rows with NaN values in numerical columns
        data = data.dropna(subset=['textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score'])

        reddit_comments_mean = data[['textblob_polarity', 'textblob_subjectivity', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compound', 'afinn_score']].mean()

        wordcloud = WordCloud(width=800, height=400, background_color='black').generate(' '.join(data['body']))
        wordcloud_image = BytesIO()
        wordcloud.to_image().save(wordcloud_image, format='PNG')
        wordcloud_image = base64.b64encode(wordcloud_image.getvalue()).decode('utf-8')

        return reddit_comments_mean, wordcloud_image
    except Exception as e:
        logging.error(f"Failed to visualize Reddit comments data: {e}")
        return pd.Series(), ""

# List of templates to load
templates = ["bootstrap", "minty", "pulse", "flatly", 'superhero', "quartz", "cyborg", "darkly", "vapor", "solar"]

# Load figure templates
load_figure_template(templates)

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.SUPERHERO])

ign_rating, user_rating = load_ign_ratings_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')))

player_count, percentage_change = load_player_counts_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')))
percentage_color = "success" if float(percentage_change) > 0 else "danger"

web_search_count = load_search_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')), 'web_search')
youtube_search_count = load_search_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')), 'youtube_search')

leaderboard_data = load_leaderboard_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')))

# Create the leaderboard table
leaderboard_table = dbc.Table.from_dataframe(
    leaderboard_data.head(10),
    striped=True,
    bordered=True,
    hover=True,
    responsive=True,
    className="table-dark"
)

# Aggregate player count by country
country_counts = leaderboard_data['country'].value_counts().reset_index()
country_counts.columns = ['country', 'player_count']

# Create the heatmap
heatmap_fig = px.choropleth(country_counts, locations="country", locationmode="country names", 
                            color="player_count", hover_name="country",
                            color_continuous_scale=px.colors.sequential.Plasma, 
                            title="Top Players by Country (Player Count)", template='superhero')

reddit_posts_mean, reddit_posts_wordcloud = visualize_reddit_posts_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')))
reddit_comments_mean, reddit_comments_wordcloud = visualize_reddit_comments_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')))

# Define the layout of the app with the finance data, IGN ratings metrics, leaderboard table, heatmap, Reddit posts analysis, and Reddit comments analysis
app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(
                html.H1("XDefiant Dashboard", className="text-center text-light my-4"),
            )
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H3("IGN Rating:", className="text-left text-light"),
                        dbc.Badge(f"{ign_rating}", color="warning", className="mb-2", style={"font-size": "24px", "padding": "10px 20px"}),
                        html.H3("User Rating:", className="text-left text-light"),
                        dbc.Badge(f"{user_rating}", color="warning", className="mb-2", style={"font-size": "24px", "padding": "10px 20px"})
                    ],
                    width=2
                ),
                dbc.Col(
                    [
                        html.H3("Player Count:", className="text-left text-light"),
                        dbc.Badge(f"{player_count}", color="primary", className="mb-2", style={"font-size": "24px", "padding": "10px 20px"}),
                        html.H3("Percentage Change:", className="text-left text-light"),
                        dbc.Badge(f"{percentage_change}%", color="success" if float(percentage_change) > 0 else "danger", className="mb-2", style={"font-size": "24px", "padding": "10px 20px"})
                    ],
                    width=2
                ),
                dbc.Col(
                    [
                        html.H3("Google Searches:", className="text-left text-light"),
                        dbc.Badge(f"{web_search_count}", color="info", className="mb-2", style={"font-size": "24px", "padding": "10px 20px"}),
                        html.H3("YouTube Searches:", className="text-left text-light"),
                        dbc.Badge(f"{youtube_search_count}", color="danger", className="mb-2", style={"font-size": "24px", "padding": "10px 20px"})
                    ],
                    width=2
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(
                        id='candlestick-chart',
                        figure=visualize_finance_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'preprocessed_data.h5')))
                    ),
                    width=6
                ),
                dbc.Col(
                    dcc.Graph(
                        id='heatmap',
                        figure=heatmap_fig
                    ),
                    width=6
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H3("Top 10 Players", className="text-center text-light my-4"),
                        leaderboard_table
                    ],
                    width=12
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H3("Reddit Posts Sentiment Analysis:", className="text-left text-light"),
                        html.Div(
                            [
                                dbc.Badge(f"Polarity: {reddit_posts_mean['textblob_polarity']:.2f}", color="primary", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"Subjectivity: {reddit_posts_mean['textblob_subjectivity']:.2f}", color="secondary", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Negative: {reddit_posts_mean['vader_neg']:.2f}", color="danger", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Neutral: {reddit_posts_mean['vader_neu']:.2f}", color="warning", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Positive: {reddit_posts_mean['vader_pos']:.2f}", color="success", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Compound: {reddit_posts_mean['vader_compound']:.2f}", color="info", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"AFINN Score: {reddit_posts_mean['afinn_score']:.2f}", color="dark", className="m-1", style={"font-size": "16px", "padding": "5px 10px"})
                            ]
                        ),
                        html.H3("Word Cloud:", className="text-left text-light mt-4"),
                        html.Img(src=f"data:image/png;base64,{reddit_posts_wordcloud}", className="img-fluid")
                    ],
                    width=12
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H3("Reddit Comments Sentiment Analysis:", className="text-left text-light"),
                        html.Div(
                            [
                                dbc.Badge(f"Polarity: {reddit_comments_mean['textblob_polarity']:.2f}", color="primary", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"Subjectivity: {reddit_comments_mean['textblob_subjectivity']:.2f}", color="secondary", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Negative: {reddit_comments_mean['vader_neg']:.2f}", color="danger", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Neutral: {reddit_comments_mean['vader_neu']:.2f}", color="warning", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Positive: {reddit_comments_mean['vader_pos']:.2f}", color="success", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"VADER Compound: {reddit_comments_mean['vader_compound']:.2f}", color="info", className="m-1", style={"font-size": "16px", "padding": "5px 10px"}),
                                dbc.Badge(f"AFINN Score: {reddit_comments_mean['afinn_score']:.2f}", color="dark", className="m-1", style={"font-size": "16px", "padding": "5px 10px"})
                            ]
                        ),
                        html.H3("Word Cloud:", className="text-left text-light mt-4"),
                        html.Img(src=f"data:image/png;base64,{reddit_comments_wordcloud}", className="img-fluid")
                    ],
                    width=12
                )
            ]
        )
    ],
    fluid=True,
    className="dbc superhero"
)


# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)










