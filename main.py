import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import auth

import os
import datetime
import csv
import pandas as pd
import csv

# Function to obtain and print all collections and their document data
def print_all_collections_data(db):
    # List all the collections
    collections = db.collections()
    
    for collection in collections:
        print(f'Collection ID: {collection.id}')


# Function to obtain the info of the categories and turn it into pandas dataframe
# Categories data:
# category_id: str
# category_name: str
def categories_to_dataframe(db):
    # Get the categories collection
    categories = db.collection('categories')
    
    # Get all the documents in the collection
    docs = categories.stream()
    
    # Create a list to store the data
    data = []
    
    # Write the data to the list
    for doc in docs:
        category_data = doc.to_dict()
        data.append({
            'category_id': doc.id,
            'category_name': category_data['name']
        })
    
    # Create a pandas dataframe from the data
    df = pd.DataFrame(data)
    
    return df


# Function to obtain the info of the reviews and turn it into pandas dataframe
# Reviews data:
# review_id: str
# user_id: str
# title: str
# content: str
# date: timestamp
# imageUrl: str (It may not exist)
# ratings (Map)
#     - cleanliness: int
#     - foodQuality: int
#     - service: int
#     - waitTime: int
# categories (List) This list needs to be a separate pandas dataframe that matches the review_id with the category_name
def reviews_to_dataframe(db):
    # Get the reviews collection
    reviews = db.collection('reviews')
    
    # Get all the documents in the collection
    docs = reviews.stream()
    
    # Create a list to store the data
    data = []
    categories_reviews_data = []
    
    # Write the data to the list
    for doc in docs:
        review_data = doc.to_dict()
        data.append({
            'review_id': doc.id,
            'user_id': review_data['user'],
            'title': review_data.get('title',None),
            'content': review_data.get('content',None),
            'date': review_data['date'],
            'imageUrl': review_data.get('imageUrl', None),
            'cleanliness': review_data['ratings']['cleanliness'],
            'foodQuality': review_data['ratings']['foodQuality'],
            'service': review_data['ratings']['service'],
            'waitTime': review_data['ratings']['waitTime']
        })
        
        for category_id in review_data['selectedCategories']:
            categories_reviews_data.append({
                'review_id': doc.id,
                'category_id': category_id
            })

    # Create a pandas dataframe from the data
    reviews_df = pd.DataFrame(data)
    categories_reviews_df = pd.DataFrame(categories_reviews_data)

    return reviews_df, categories_reviews_df


# Function to obtain the info of the spots and turn it into a pandas dataframe
# restaurant_id: str
# latitude: double
# longitude: double
# name: str
# price: str
# categories (List) This list needs to be a separate pandas dataframe that matches the restaurant_id with the category_name
# userReviews (List) This list needs to be a separate pandas dataframe that matches the restaurant_id with the review_id (In firestore it is a reference, so get the id)
def spots_to_dataframe(db):
    # Get the spots collection
    spots = db.collection('spots')

    unfinished_reviews = db.collection('unfinishedReviews')

    
    # Get all the documents in the collection
    docs = spots.stream()
    docs_unfinished = unfinished_reviews.stream()

    unfinished_reviews_data = {}

    for doc in docs_unfinished:
        review_data = doc.to_dict()
        unfinished_reviews_data[review_data["spot"]] = review_data["count"]

    
    # Create a list to store the data
    data = []
    categories_spots_data = []
    user_reviews_data = []
    
    # Write the data to the list
    for doc in docs:
        spot_data = doc.to_dict()
        data.append({
            'restaurant_id': doc.id,
            'latitude': spot_data['location-arr'][0],
            'longitude': spot_data['location-arr'][1],
            'name': spot_data['name'],
            'price': spot_data['price'],
            'unfinished_reviews': unfinished_reviews_data.get(spot_data['name'], 0)
        })
        
        for category_id in spot_data['categories']:
            categories_spots_data.append({
                'restaurant_id': doc.id,
                'category_id': category_id
            })
        
        for review_ref in spot_data['reviewData']['userReviews']:
            review_id = review_ref.id
            user_reviews_data.append({
                'restaurant_id': doc.id,
                'review_id': review_id
            })
    
    # Create pandas dataframes from the data
    spots_df = pd.DataFrame(data)
    categories_spots_df = pd.DataFrame(categories_spots_data)
    user_reviews_df = pd.DataFrame(user_reviews_data)
    
    return spots_df, categories_spots_df, user_reviews_df


def search_terms_to_dataframe(db):
    search_terms_collection = db.collection('searchTerms')
    
    docs = search_terms_collection.stream()
    
    search_terms_dict = {}
    
    for i, doc in enumerate(docs):
        doc_data = doc.to_dict()
        for key, value in doc_data.items():
            # If the key already exists, append the list to the corresponding column
            if key in search_terms_dict:
                search_terms_dict[key].append(value)
            # Otherwise, create a new key with the list
            else:
                search_terms_dict[key] = [value] * i + [value]
    
    search_terms_df = pd.DataFrame(search_terms_dict)
    
    search_terms_df = search_terms_df.map(lambda x: ', '.join(x) if isinstance(x, list) and len(x) > 1 else x)
    
    for col in search_terms_df.columns:
        if search_terms_df[col].apply(lambda x: isinstance(x, list) and len(x) == 1).any():
            search_terms_df[col] = search_terms_df[col].apply(lambda x: x[0] if isinstance(x, list) else x)
    
    return search_terms_df

def bug_reports_to_dataframe(db):
    bug_reports_collection = db.collection('bugReports')

    docs = bug_reports_collection.stream()

    bug_reports_data = []

    for doc in docs:
        bug_report_data = doc.to_dict()
        bug_reports_data.append({
            'bugType': bug_report_data['bugType'],
            'date': bug_report_data['date'],
            'description': bug_report_data['description'],
            'severityLevel': bug_report_data['severityLevel'],
            'stepsToReproduce': bug_report_data['stepsToReproduce'],
        })
    
    bug_reports_df = pd.DataFrame(bug_reports_data)
    return bug_reports_df


# Function to obtain the info of the categories dataframe and turn it into a csv file
def dataframe_to_csv(data, folder_path, filename):


    # Save categories dataframe to csv
    data.to_csv(f"{folder_path}/{filename}.csv", index=False)

    # Print to indicate it saved the file
    print(f"The CSV {filename} has been created and loaded")


def bookmarks_usage_to_dataframe(db):
    users = get_all_user_ids()

    bookmarks_usage = db.collection('bookmarksUsage')

    docs = bookmarks_usage.stream()

    data = []
    for doc in docs:
        bookmarks_data = doc.to_dict()
        data.append({
            'user_id': bookmarks_data['userId'],
            'using': bookmarks_data['usage'],
        })
    
    for user in users:
        if user not in [d['user_id'] for d in data]:
            data.append({
                'user_id': user,
                'using': False
            })
    
    df = pd.DataFrame(data)
    return df

def get_all_user_ids():
    ids = []
    users = auth.list_users()
    for user in users.iterate_all():
        email = user.email
        ids.append(email.split('@')[0])
    return ids

# Create main function
def main(): 
    #Print initializing message
    print("Connecting to firebase... ")

    #Connect to firebase
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    # Print all collections in firestore
    print_all_collections_data(db)

    # Get the categories data
    categories_data = categories_to_dataframe(db)
    # Get the reviews data
    reviews_data, categories_reviews_data = reviews_to_dataframe(db)
    # Get the spots data
    spots_data, categories_spots_data, spots_reviews_data = spots_to_dataframe(db)
    # Get the bookmarks usage data
    bookmarks_usage_data = bookmarks_usage_to_dataframe(db)
    # Get the search terms data
    search_terms_data = search_terms_to_dataframe(db)
    # Get bug reports data 
    bug_reports_data = bug_reports_to_dataframe(db)

    # Create folder file path (results/timestamp of the day)
    #timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_path = f"results"
    os.makedirs(folder_path, exist_ok=True)

    # Check if the folder exists
    if os.path.exists(folder_path):
        # Get all the files in the folder
        files = os.listdir(folder_path)
        
        # Delete each file in the folder
        for file in files:
            file_path = os.path.join(folder_path, file)
            os.remove(file_path)
        
        print("All files inside the folder have been deleted.")
    else:
        print("The folder does not exist.")

    # Transform data to csv
    dataframe_to_csv(categories_data, folder_path, "categories")
    dataframe_to_csv(reviews_data, folder_path, "reviews")
    dataframe_to_csv(spots_data, folder_path, "spots")
    dataframe_to_csv(categories_reviews_data, folder_path, "categories_reviews")
    dataframe_to_csv(categories_spots_data, folder_path, "categories_spots")
    dataframe_to_csv(spots_reviews_data, folder_path, "spots_reviews")
    dataframe_to_csv(bookmarks_usage_data, folder_path, "bookmarks_usage")
    dataframe_to_csv(search_terms_data, folder_path, "search_terms")
    dataframe_to_csv(bug_reports_data, folder_path, "bug_reports")




if __name__ == "__main__":
    main()

