from flask import Flask, render_template
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import plotly.express as px
from dotenv import load_dotenv
import os

app = Flask(__name__)
load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

DATABASE_URI = f'postgresql://{db_user}:{db_password}@localhost:5432/crypto_raw'

@app.route('/')
def index():
    # Create SQLAlchemy engine
    engine = create_engine(DATABASE_URI)

    # Query data from PostgreSQL
    query = """
    SELECT name, symbol, price, percent_change_1h, percent_change_24h, date
    FROM crypto_trends
    """
    df = pd.read_sql(query, engine)
    
    # Process data for bar chart
    bar_data = df.nlargest(10, 'percent_change_24h')
    bar_fig = px.bar(bar_data, x='name', y='percent_change_24h', title='Top 10 Cryptos by 24h Change')
    bar_chart = bar_fig.to_html(full_html=False)
    
    # Process data for pie chart
    pie_data = df.nlargest(10, 'price')
    pie_fig = px.pie(pie_data, values='price', names='name', title='Top 10 Cryptos by Price', labels={'price': 'Price', 'name': 'Cryptocurrency'})
    pie_fig.update_traces(textposition='inside', textinfo='percent+label')
    pie_fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
    pie_chart = pie_fig.to_html(full_html=False)
    
    return render_template('index.html', tables=[df.to_html(classes='data table table-striped table-bordered')], titles=df.columns.values, bar_chart=bar_chart, pie_chart=pie_chart)

if __name__ == '__main__':
    app.run(debug=True)
