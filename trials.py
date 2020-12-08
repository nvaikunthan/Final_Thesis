import psycopg2
import numpy as np
import matplotlib.pyplot as plt


def connect():
    try:
        print('Connecting to database...')
        conn = psycopg2.connect(host="*********.rds.amazonaws.com", dbname="mimic",
                                user="nvaikunthan", password="*******", port="5432")
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





def patients_visit_stats(cur):

    if cur.closed:
        print("Error: cursor is closed")

    # Query database
    cur.execute("SELECT SUBJECT_ID, COUNT(DISTINCT HADM_ID) FROM diagnoses_icd \
    WHERE SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) >= 2))\
     GROUP BY SUBJECT_ID ORDER BY COUNT(DISTINCT HADM_ID) DESC; ")
    pt_rows = cur.fetchall()

    rel_patient_count = cur.rowcount
    print("Number of rows: ", rel_patient_count)

    # Summary Stats
    pt_arr = np.array(pt_rows)
    visit_nos_per_pt = pt_arr[:, 1]
    median_visits = np.median(visit_nos_per_pt)
    mean_visits = np.mean(visit_nos_per_pt)
    max10__visits = visit_nos_per_pt[0:9]
    print("Average number of visits per patient are: ", mean_visits)
    print("Median number of visits per patient are: ", median_visits)
    print("Top visit nos:", max10__visits)

    # plot distribution
    plt.hist(visit_nos_per_pt, edgecolor='black')
    plt.title("Number of Visits per Patient")
    plt.xlabel("Visits")
    plt.ylabel("Frequency")
    plt.show()


def general_visit_stats(cur):

    if cur.closed:
        print("Error: cursor is closed")

    # Query database
    cur.execute("SELECT SUBJECT_ID, HADM_ID, COUNT(DISTINCT ICD9_CODE) FROM diagnoses_icd \
    WHERE SUBJECT_ID IN (SELECT SUBJECT_ID from admissions \
    GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) > 2)) \
    GROUP BY SUBJECT_ID, HADM_ID ORDER BY COUNT(DISTINCT ICD9_CODE) DESC;  ")
    visit_rows = cur.fetchall()

    visit_count = cur.rowcount
    print("Number of rows: ", visit_count)

    visit_array = np.array(visit_rows)
    diagnoses_per_vst = visit_array[:, 2]
    median_diagnoses = np.median(diagnoses_per_vst)
    mean_diagnoses = np.mean(diagnoses_per_vst)
    max10__diagnoses = diagnoses_per_vst[0:9]
    print("Average number of diagnoses per visit are: ", mean_diagnoses)
    print("Median number of diagnoses per visit are: ", median_diagnoses)
    print("Top number of diagnoses per visit:", max10__diagnoses)

    # plot distribution
    plt.hist(diagnoses_per_vst, edgecolor='black')
    plt.title("Number of Diagnoses per Patient")
    plt.xlabel("Diagnoses")
    plt.ylabel("Frequency per Patient")
    plt.show()






def main():
    current_connection = connect()
    current_cur = current_connection[1]

    patients_visit_stats(current_cur)
    general_visit_stats(current_cur)
    close(current_connection[0], current_connection[1])

if __name__ == '__main__':
    main()

