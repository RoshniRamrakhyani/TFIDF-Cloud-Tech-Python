# TFIDF-Cloud-Tech-Python
This project calculates the TFIDF- of the data collected.  
1. TASK 1- DATA ACQUISITION
2. TASK 2- Extract, Transform and Load the data using PIG
3. TASK 3- Querying data using HIVE
4. TASK 4- Calculate TF-IDF using PYTHON
*******************************************Calculate TF-IDF using PYTHON******************************************************************
1.	Create Data for the input of TF_IDF code 
hive>create table TopPost(owneruserid int, title string);
hive>insert into TopPost;
hive>select postsdata.owneruserid, postsdata.title from posts where userid in (select postsdata.owneruserid, sum(postsdata.score) as total_score from postsdata
    >group by postsdata.owneruserid
    >order by total_score desc
    >limit 10;);
    >GROUP BY posts.owneruserid, posts.title
hive>create table USER4883 (Title string);
hive>insert into USER4883;
hive>select title from TopPost where owneruserid=4883;

Create table for all the 10 users separately and download the tables from Hadoop into the cloud storage and then to the local machine.
On jupyter notebook create the python code to calculate the per-user TF-IDF of the top 10 terms for each of the top 10 users

CODE and OUTPUT – TFIDF_Task4.ipynb  
The code uses TfidfVectorizer 
TFIDFVECTORIZER-
  TF-IDF is a statistical measure that evaluates how relevant a word is to a document in a collection of documents. This is done by multiplying two metrics: how many times a word appears in a document, and the inverse document frequency of the word across a set of documents.The TfidfVectorizer will tokenize documents, learn the vocabulary and inverse document frequency weightings, and allow you to encode new documents.
What is Vectorizer Fit_transform?
 In a sparse matrix, most of the entries are zero and hence not stored to save memory. The numbers in bracket are the index of the value in the matrix (row, column) and 1 is the value (The number of times a term appeared in the document represented by the row of the matrix). –
fit_transform(raw_documents[, y])	Learn vocabulary and idf, return document-term matrix.

stop_words{‘english’}, list, default=None
If a string, it is passed to _check_stop_list and the appropriate stop list is returned. ‘english’ is currently the only supported string value. There are several known issues with ‘english’ and you should consider an alternative (see Using stop words).

 

