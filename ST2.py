import streamlit as st
import mysql.connector
import pandas as pd

# Step 1: Database Connection
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
            port = 4000,
            user = "Fp8PXauo85Kz2dU.root",
            password = "fxHWFzWYYI5NTk6",
            database = "google_books",
            ssl_ca = "E:/bookscrapper/isrgrootx1.pem",
            ssl_verify_cert = True,
            ssl_verify_identity = True
        )
        if connection.is_connected():
            #st.success("Connected to the database successfully!")
            return connection
        else:
            st.error("Connection failed.")
            return None
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Step 2: Execute SQL Query
def execute_query(connection, query):
    if not connection or not connection.is_connected():
        st.error("MySQL connection is not available.")
        return pd.DataFrame()

    try:
        cursor = connection.cursor(dictionary=True)  # Use dictionary=True for readable column names
        cursor.execute(query)
        results = cursor.fetchall()
        return pd.DataFrame(results)  # Convert results to a DataFrame
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()

# Step 3: Define Queries
queries = {
    "Check Availability of eBooks vs Physical Books": """
        SELECT 
            isEbook AS ebook_available,
            COUNT(*) AS book_count
        FROM books
        GROUP BY isEbook;
    """,
    "Find the Publisher with the Most Books Published": """
        SELECT 
            book_authors AS publisher,
            COUNT(*) AS total_books
        FROM books
        GROUP BY book_authors
        ORDER BY total_books DESC
        LIMIT 1;
    """,
    "Identify the Publisher with the Highest Average Rating": """
        SELECT 
            book_authors AS publisher,
            AVG(averageRating) AS avg_rating
        FROM books
        WHERE averageRating IS NOT NULL
        GROUP BY book_authors
        ORDER BY avg_rating DESC
        LIMIT 1;
    """,
    "Get the Top 5 Most Expensive Books by Retail Price": """
        SELECT 
            book_title,
            amount_retailPrice AS retail_price,
            currencyCode_retailPrice
        FROM books
        WHERE amount_retailPrice IS NOT NULL
        ORDER BY retail_price DESC
        LIMIT 5;
    """,
    "Find Books Published After 2010 with at Least 500 Pages": """
        SELECT 
            book_title,
            pageCount,
            year
        FROM books
        WHERE CAST(year AS UNSIGNED) > 2010 AND pageCount >= 500;
    """,
    "List Books with Discounts Greater than 20%": """
        SELECT 
            book_title,
            amount_retailPrice AS retail_price,
            amount_listPrice AS sale_price,  -- Assuming listPrice is the sale price
            (1 - (amount_listPrice / amount_retailPrice)) * 100 AS discount_percentage  -- Calculate discount percentage
        FROM books
            WHERE amount_retailPrice IS NOT NULL 
            AND amount_listPrice IS NOT NULL  -- Ensure both prices are available
            AND (1 - (amount_listPrice / amount_retailPrice)) * 100 > 20
            ORDER BY discount_percentage DESC;
    """,
    "Find Average Page Count for eBooks and Physical Books": """
        SELECT 
            CASE WHEN isEbook = 1 THEN 'eBooks'
            ELSE 'Physical Books'
            END AS book_type,
            AVG(pageCount) AS avg_page_count
            FROM books
            WHERE pageCount IS NOT NULL
            GROUP BY book_type;
    """,
    "Find the Top 3 Authors with the Most Books": """
        SELECT book_authors AS author,
            COUNT(*) AS total_books
            FROM books
            GROUP BY book_authors
            ORDER BY total_books DESC
            LIMIT 3;
    
    """,
    "Find Publishers with More than 10 Books": """
    SELECT book_authors AS publisher, COUNT(*) AS book_count
        FROM books
        GROUP BY book_authors
        HAVING COUNT(*) > 10;
    
    """,

    "Find the Top 10 Categories with the Highest Average Page Count": """
    SELECT categories, AVG(pageCount) AS avg_page_count
    FROM books
    GROUP BY categories
    ORDER BY avg_page_count DESC
    LIMIT 10  ;
    """,
    "Retrieve Books with More than 3 Authors": """
        SELECT book_title,book_authors
        FROM books
        WHERE LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) > 2 ;

    """,
    "Books with Ratings Count Greater Than the Average": """
        SELECT 
        book_title,
        ratingsCount
    FROM books
    WHERE ratingsCount > (
    SELECT AVG(ratingsCount)
    FROM books
    WHERE ratingsCount IS NOT NULL )
    ORDER BY ratingsCount DESC
    LIMIT 10;
    
    """,
"Books with the Same Author Published in the Same Year":"""
    SELECT 
    book_title,
    book_authors AS author,
    year
    FROM books
    WHERE (book_authors, year) IN (
    SELECT 
        book_authors,
        year
    FROM books
    WHERE year IS NOT NULL AND year != ''  -- Exclude NULL and empty year values
    GROUP BY book_authors, year
    HAVING COUNT(*) > 1
)
    ORDER BY  year,book_authors
    limit 10;
    """,

    "Books with a Specific Keyword <Science> in the Title": """
        SELECT 
        book_title,
        book_authors AS author,
        year
        FROM books
        WHERE book_title LIKE '%Science%'  -- Replace 'Science' with your desired keyword
        ORDER BY book_title;
    """,
    "Year with the Highest Average Book Price": """
    SELECT 
        year,
        AVG(amount_retailPrice) AS avg_price
        FROM books
        WHERE amount_retailPrice IS NOT NULL  -- Exclude rows with NULL retail prices
        GROUP BY year
        ORDER BY avg_price DESC
        LIMIT 1;
    """,
    "Count Authors Who Published 3 Consecutive Years": """
    SELECT 
    book_authors AS author,
    COUNT(DISTINCT year) AS consecutive_years
    FROM books b1
    WHERE year IS NOT NULL AND year != ''
    AND EXISTS (
        SELECT 1
        FROM books b2
        WHERE b1.book_authors = b2.book_authors
        AND CAST(b2.year AS UNSIGNED) = CAST(b1.year AS UNSIGNED) + 1
        AND EXISTS (
            SELECT 1
            FROM books b3
            WHERE b2.book_authors = b3.book_authors
            AND CAST(b3.year AS UNSIGNED) = CAST(b2.year AS UNSIGNED) + 1
        )
    )
    GROUP BY book_authors
    HAVING COUNT(DISTINCT year) >= 3;
    """,
    "find authors who have published books in the same year but under different publishers": """
    # SELECT 
    #     book_authors AS author,
    #     year,
    #     COUNT(*) AS book_count
    # FROM books
    # WHERE year IS NOT NULL AND year != ''
    # GROUP BY book_authors, year
    # HAVING COUNT(DISTINCT book_authors) > 1
    # ORDER BY author, year;

        SELECT 
        book_authors AS author,
        year,
        COUNT(*) AS book_count
        FROM books
        WHERE year IS NOT NULL AND year != ''
        GROUP BY book_authors, year
        HAVING COUNT(*) > 1
        ORDER BY author, year
        limit 10 ;

    """,
    "average amount_retailPrice of eBooks and physical books": """
    SELECT
        IFNULL(AVG(CASE WHEN isEbook = 1 THEN amount_retailPrice END), 0) AS avg_ebook_price,
        IFNULL(AVG(CASE WHEN isEbook = 0 THEN amount_retailPrice END), 0) AS avg_physical_price
    FROM books;

    """,
    "Books with averageRating more than two standard deviations away from the average rating of all books": """
    WITH rating_stats AS (
        SELECT 
            AVG(averageRating) AS avg_rating,
            STDDEV(averageRating) AS std_dev_rating
        FROM books
        WHERE averageRating IS NOT NULL
    )

    SELECT 
        book_title,
        averageRating,
        ratingsCount
    FROM books
    WHERE averageRating IS NOT NULL
    AND (
        averageRating > (SELECT avg_rating + 2 * std_dev_rating FROM rating_stats)
        OR 
        averageRating < (SELECT avg_rating - 2 * std_dev_rating FROM rating_stats)
    )
    ORDER BY averageRating DESC;
    """,
    "Publisher with highest average rating among its books, but only for publishers that have published more than 10 books.": """
    SELECT 
        book_authors AS publisher,
        AVG(averageRating) AS average_rating,
        COUNT(*) AS number_of_books
    FROM books
    WHERE averageRating IS NOT NULL
    GROUP BY book_authors
    HAVING COUNT(*) > 10
    ORDER BY average_rating DESC
    LIMIT 1;

    """
    # Add more queries here as needed...
}

# Step 4: Streamlit App
# def main():
#     st.title("Muthu's BookScape Explorer")
#     st.write("Explore book data using predefined SQL queries.")

#     # Database connection
#     connection = connect_to_db()
#     if not connection:
#         st.stop()  # Stop the app if connection is not established
    
#     #Dropdown menu for query selection
#     query_name = st.selectbox(
#         "Select a Query to Run:",
#         options=list(queries.keys())
#     )

#     if st.button("Run Query"):   
#         query = queries[query_name]
#         st.write(f"**Query Selected:** {query_name}")
#         #st.code(query, language="sql")
        
#         # Execute the selected query and display results
#         results_df = execute_query(connection, query)
        
#         if not results_df.empty:
#             st.write(f"**Results for: {query_name}**")
#             st.dataframe(results_df)  # Display results as a table
#         else:
#             st.warning("No results found for this query.")



#     # Close the database connection
#     connection.close()
def style_table_headers():
    st.markdown(
        """
        <style>
        /* Make table headers bold */
        thead th {
            font-weight: bold !important;
        }
        /* Optionally, hide the first index column if needed */
        tbody tr th {
            display: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
def main():
    st.set_page_config(
        page_title="Muthu's BookScape Explorer",
        page_icon="📚",
        layout="wide"

    )
    def add_bg_color():
        st.markdown(
        """
        <style>
        /* Change background color */
        .stApp {
            background-color: #f0f8ff; /* Light blue color */
        }
        /* Optional: Adjust text color */
        .stApp {
            color: #333333; /* Darker text for contrast */
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# Apply the custom CSS
    add_bg_color()

    st.title("📚 Muthu's BookScape Explorer")
    st.markdown("**Explore rich insights about books using predefined SQL queries. 📊**")
    
    # Database connection
    connection = connect_to_db()
    if not connection:
        st.stop()  # Stop the app if connection is not established

    # Sidebar for query selection
    st.sidebar.title("Navigation")
    query_name = st.sidebar.selectbox(
        "**Select a Query to Run:**",
        options=list(queries.keys())
    )

    st.sidebar.markdown("---")
    #st.sidebar.info("Select a query to display insights or explore further.")
    
    # Query description
    query_descriptions = {
        "Check Availability of eBooks vs Physical Books": "Analyze the availability of eBooks vs physical books.",
        "Find the Publisher with the Most Books Published": "Identify the publisher who has published the most books.",
        "Identify the Publisher with the Highest Average Rating": "Discover the publisher with the highest average book rating.",
        # Add more descriptions for other queries
    }

    st.write(f"### Query: {query_name}")
    if query_name in query_descriptions:
        st.write(query_descriptions[query_name])

    st.markdown("---")

    # Execute query and display results
    if st.sidebar.button("🔎 Run Query"):   
        query = queries[query_name]
        
        # Execute the selected query and display results
        results_df = execute_query(connection, query)
        
        if not results_df.empty:
            # Add Serial Numbers to Results
            results_df.insert(0, 'S.No', range(1, len(results_df) + 1))
            
            st.write(f"### Results for: {query_name}")
            
            # Display results with enhanced formatting
            st.table(results_df)  # Enhanced table display
            # new title bold

            
            
            # title end
            # Download button for exporting results
            # csv = results_df.to_csv(index=False)
            # st.download_button(
            #     label="📥 Download Results as CSV",
            #     data=csv,
            #     file_name=f"{query_name.replace(' ', '_')}_results.csv",
            #     mime="text/csv"
            # )
        else:
            st.warning("No results found for this query.")

    # Close the database connection
    connection.close()


if __name__ == "__main__":
    main()
