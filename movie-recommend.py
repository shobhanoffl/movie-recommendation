from google.cloud import bigquery
from google.oauth2 import service_account
import mysql.connector

# SERVICE_ACCOUNT_JSON = 

credentials = service_account.Credentials.from_service_account_file(
    "C:\\Users\\rshob\\Downloads\\ecommerce\\dataeng-383420-32d4b9dc88df.json"  , scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
# client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


cnx = mysql.connector.connect(user='root', password='',host='localhost', database='movies')
cursor = cnx.cursor()

# Get Dataset
query = ('SELECT * FROM movies')
cursor.execute(query)

movies = []
for (id, title, rating, actors, genre) in cursor:
    movies.append((id, title, rating, actors, genre))

# Get History
query = ('SELECT * FROM history')
cursor.execute(query)

history = []
for (uid,mid,rating) in cursor:
    history.append((uid,mid, rating))
# print(history)

def getTopMovies(user_id):
    movie_ratings = {}
    for h in history:
        user_id, movie_id, rating = h
        if user_id not in movie_ratings:
            movie_ratings[user_id] = {}
        movie_ratings[user_id][movie_id] = rating

    user_ratings = movie_ratings.get(user_id, {})
    favorite_movies = sorted(user_ratings.keys(), key=lambda k: user_ratings[k], reverse=True)

    # create a list of all movies the user hasn't seen
    unseen_movies = [movie for movie in movies if movie[0] not in user_ratings]

    # find the top 10 movies based on overall ratings
    top_movies = sorted(unseen_movies, key=lambda movie: float(movie[2]), reverse=True)[:10]

    # print the top 10 movies
    final_list=[]
    for movie in top_movies:
        final_list.append(movie[1])  # print the movie title

    return final_list

users = set()
for h in history:
    user_id = h[0]
    users.add(user_id)

unique_users = sorted(list(users))

dwList=[]
for i in unique_users:
    dwList.append((i,getTopMovies(i)))

print(dwList)


# set up table schema
schema = [
    bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("movies", "STRING", mode="REPEATED"),
]

# create the table
table_ref = client.dataset('ecommerce').table('Top Movies')
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)  # API request

# insert data into the table
rows_to_insert = []
for data_row in dwList:
    user_id = data_row[0]
    movies = data_row[1]
    rows_to_insert.append({"user_id": user_id, "movies": movies})
    
client.insert_rows_json(table, rows_to_insert)  # API request

print("Data successfully loaded into BigQuery table: {}".format(table_id))



# Functions
# getTopActors

# # Define BigQuery table schema
# schema = [
#     bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
#     bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
#     bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED')
# ]

# # Define BigQuery table and insert rows
# table_id = 'your_project.your_dataset.your_table'
# table = bigquery.Table(table_id, schema=schema)
# table.insert_rows(rows)

# # Close MySQL connection
# cursor.close()
# cnx.close()
