from flask import Flask, render_template, request
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from flask_redis import FlaskRedis
import redis

app = Flask(__name__)
app.config['REDIS_URL'] = 'redis://my-redis-container:6379/0'
redis_client = FlaskRedis(app)

df = pd.read_csv('books.csv', on_bad_lines='skip', encoding='latin-1', sep=',')
df.duplicated(subset='title').sum()
df = df.drop_duplicates(subset='title')
sample_size = 4000
df = df.sample(n=sample_size, replace=False, random_state=490)

def clean_text(author):
    result = str(author).lower()
    return(result.replace(' ',''))

df['authors'] = df['authors'].apply(clean_text)
df['title'] = df['title'].str.lower()
df['published_year'] = df['published_year'].astype(str).str.lower()
df2 = df.drop(['isbn13','isbn10','thumbnail','description','average_rating', 'num_pages', 'ratings_count'],axis=1)
df2['data'] = df2[df2.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)

vectorizer = CountVectorizer()
vectorized = vectorizer.fit_transform(df2['data'])
similarities = cosine_similarity(vectorized)
df = pd.DataFrame(similarities, columns=df['title'], index=df['title']).reset_index()

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        input_book = request.form['input_book']
        recommendations = get_recommendations(input_book)
        return render_template('index.html', message="Recomendaciones generadas con exito!")
    return render_template('index.html')

def get_recommendations(input_book):
    stored_recommendations = redis_client.get(input_book)
    if stored_recommendations:
        return stored_recommendations.decode('utf-8').split(',')
    else:
        if input_book not in df['title'].values:
            similar_books = find_similar_books(input_book)
            if similar_books:
                redis_client.set(input_book, ','.join(similar_books))
                return similar_books
        else:
            recommendations = pd.DataFrame(df.nlargest(11, input_book)['title'])
            recommendations = recommendations[recommendations['title'] != input_book]
            recommended_books = recommendations['title'].values.tolist()
            redis_client.set(input_book, ','.join(recommended_books))
            return recommended_books


def find_similar_books(input_book):
    input_book_vector = vectorizer.transform([input_book])
    similarities = cosine_similarity(input_book_vector, vectorized)
    similar_books_indices = similarities.argsort()[0][-11:-1]
    similar_books = df['title'].iloc[similar_books_indices].values.tolist()
    return similar_books


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
