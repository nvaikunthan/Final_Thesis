import psycopg2
import spacy as sp
import numpy as np
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import CountVectorizer


def connect():
    try:
        print('Connecting to database...')
        conn = psycopg2.connect(host="*******.rds.amazonaws.com", dbname="mimic",
                                user="nvaikunthan", password="*********", port="5432")
        cur = conn.cursor()
        cur.execute("set search_path to mimiciii")
        print('Connected to database and correct schema')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return [conn, cur]


def close(conn, cur):
    cur.close()
    conn.close()
    print('Connection closed')


def notes_overview(cur):

    if cur.closed:
        print("Error: cursor is closed")
    cur.execute("SELECT DISTINCT CATEGORY, COUNT(TEXT) FROM noteevents GROUP BY CATEGORY")
    cat_counts = cur.fetchall()
    num_of_cat = cur.rowcount
    print("Number of note categories: ", num_of_cat)

    cat_array = np.array(cat_counts)
    for i in range(num_of_cat):
        print(str(cat_array[i, 0]) + " " + str(cat_array[i, 1]))


def notes_distribution(cur):

    if cur.closed:
        print("Error: cursor is closed")
    cur.execute("SELECT ADMISSIONS.SUBJECT_ID, COUNT(DISTINCT TEXT)  FROM noteevents \
                INNER JOIN admissions ON noteevents.hadm_id = admissions.hadm_id WHERE \
                (category='Physician ' OR category='Nursing' OR category='Nursing/other') AND (ADMISSIONS.SUBJECT_ID IN \
                (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) >= 2))) \
                 GROUP BY ADMISSIONS.SUBJECT_ID ORDER BY COUNT(DISTINCT TEXT) DESC")
    dist_rows = cur.fetchall()
    num_of_visitors = cur.rowcount
    print("Number of valid subjects", num_of_visitors)

    dist_array = np.array(dist_rows)
    note_dist_array = dist_array[:, 1]
    dist_mean = np.mean(note_dist_array)
    dist_median = np.median(note_dist_array)
    print(dist_mean)
    print(dist_median)

    plt.hist(note_dist_array, edgecolor='black')
    plt.title("Number of Notes per Visit")
    plt.xlabel("Notes")
    plt.ylabel("Frequency per Visit")
    plt.show()




def main():
    current_connection = connect()
    current_cur = current_connection[1]
    # doc_word_matrix_test(current_cur)
    notes_overview(current_cur)
    notes_distribution(current_cur)
    close(current_connection[0], current_connection[1])

if __name__ == '__main__':
    main()

