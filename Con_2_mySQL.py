
#!pip install mysql-connector-python
import requests
import mysql.connector  # For MySQL, use psycopg2 for PostgreSQL
import json

# Step 1: Database Connection
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            # host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",        # Replace with your database host
            # user="Fp8PXauo85Kz2dU.root",    # Replace with your database username
            # password="fxHWFzWYYI5NTk6e",# Replace with your database password
            # database="google_books" # Replace with your database name
            host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
            port = 4000,
            user = "Fp8PXauo85Kz2dU.root",
            password = "fxHWFzWYYI5NTk6",
            database = "google_books",
            ssl_ca = "E:/bookscrapper/isrgrootx1.pem",
            ssl_verify_cert = True,
            ssl_verify_identity = True
        )
        print("Connected to the database successfully!")
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Step 2: Fetch Data from Google Books API
def fetch_books(api_key, search_term, max_results=40):
    base_url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": search_term,
        "key": api_key,
        "maxResults": max_results,
        "startIndex": 0
    }
    all_books = []
    try:
        for start_index in range(0, 1000, max_results):  # Pagination for up to 1000 books
            params["startIndex"] = start_index
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                books = data.get("items", [])
                all_books.extend(books)
                if len(books) < max_results:  # Stop if no more results
                    break
            else:
                print(f"Error fetching data: {response.status_code}")
                break
        return all_books
    except Exception as e:
        print(f"Error: {e}")
        return []

# Step 3: Process Data
def process_book_data(book):
    volume_info = book.get("volumeInfo", {})
    sale_info = book.get("saleInfo", {})
    identifiers = volume_info.get("industryIdentifiers", [])
    industry_ids = ",".join([f"{id['type']}:{id['identifier']}" for id in identifiers])

    return {
        "book_id": book.get("id"),
        "book_title": volume_info.get("title", "N/A"),
        "book_subtitle": volume_info.get("subtitle", ""),
        "book_authors": ",".join(volume_info.get("authors", [])),
        "book_description": volume_info.get("description", ""),
        "industryIdentifiers": industry_ids,
        "text_readingModes": volume_info.get("readingModes", {}).get("text", False),
        "image_readingModes": volume_info.get("readingModes", {}).get("image", False),
        "pageCount": volume_info.get("pageCount", 0),
        "categories": ",".join(volume_info.get("categories", [])),
        "language": volume_info.get("language", ""),
        "imageLinks": volume_info.get("imageLinks", {}).get("thumbnail", ""),
        "ratingsCount": volume_info.get("ratingsCount", 0),
        "averageRating": volume_info.get("averageRating", 0.0),
        "country": sale_info.get("country", ""),
        "saleability": sale_info.get("saleability", ""),
        "isEbook": sale_info.get("isEbook", False),
        "amount_listPrice": sale_info.get("listPrice", {}).get("amount", None),
        "currencyCode_listPrice": sale_info.get("listPrice", {}).get("currencyCode", None),
        "amount_retailPrice": sale_info.get("retailPrice", {}).get("amount", None),
        "currencyCode_retailPrice": sale_info.get("retailPrice", {}).get("currencyCode", None),
        "buyLink": sale_info.get("buyLink", ""),
        "year": volume_info.get("publishedDate", "").split("-")[0]
    }

# Step 4: Insert Data into SQL Database
def insert_books_to_db(connection, books):
    cursor = connection.cursor()
    query = """
    INSERT INTO books (
        book_id, book_title, book_subtitle, book_authors, book_description,
        industryIdentifiers, text_readingModes, image_readingModes, pageCount,
        categories, language, imageLinks, ratingsCount, averageRating, country,
        saleability, isEbook, amount_listPrice, currencyCode_listPrice,
        amount_retailPrice, currencyCode_retailPrice, buyLink, year
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        book_title=VALUES(book_title), book_authors=VALUES(book_authors),
        averageRating=VALUES(averageRating), ratingsCount=VALUES(ratingsCount);
    """
    for book in books:
        try:
            cursor.execute(query, (
                book["book_id"], book["book_title"], book["book_subtitle"], book["book_authors"], book["book_description"],
                book["industryIdentifiers"], book["text_readingModes"], book["image_readingModes"], book["pageCount"],
                book["categories"], book["language"], book["imageLinks"], book["ratingsCount"], book["averageRating"],
                book["country"], book["saleability"], book["isEbook"], book["amount_listPrice"],
                book["currencyCode_listPrice"], book["amount_retailPrice"], book["currencyCode_retailPrice"],
                book["buyLink"], book["year"]
            ))
            connection.commit()
        except Exception as e:
            print(f"Error inserting book {book['book_id']}: {e}")
    cursor.close()

# Main Function
def main():
    API_KEY = "AIzaSyDAYCMksQOTGw02bT2noy3D7hhaFJkrSCE"  # Replace with your Google Books API key
    SEARCH_TERM = "Data Science"       # Replace with your search term

    # Connect to Database
    connection = connect_to_db()
    if connection is None:
        return

    # Fetch and Process Books
    raw_books = fetch_books(API_KEY, SEARCH_TERM)
    processed_books = [process_book_data(book) for book in raw_books]

    # Insert Books into Database
    insert_books_to_db(connection, processed_books)

    # Close Database Connection
    connection.close()
    print("Data insertion completed!")

if __name__ == "__main__":
    main()


