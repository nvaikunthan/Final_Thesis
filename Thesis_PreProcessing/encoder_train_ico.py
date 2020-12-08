import numpy as np
from trials_note import connect
from trials_note import close
from ico_codes import ico_map



def ico_code_ret_encoder(cur):
    if cur.closed:
        print("Error: cursor is closed")
    cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, COUNT(DISTINCT TEXT) \
    FROM noteevents INNER JOIN admissions ON noteevents.hadm_id = admissions.hadm_id \
    WHERE (category='Physician ' OR category='Nursing' OR category='Nursing/other') \
    AND (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) = 1))) \
    GROUP BY ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID ORDER BY ADMISSIONS.SUBJECT_ID")

    cur_rows = cur.fetchall()
    num_of_visitors = cur.rowcount

    code_dict = {}

    for row in cur_rows:
        key = row[0]
        val = row[1]

        many_hot_vec = np.zeros([943,])
        if key not in code_dict:
            code_dict[key] = {val: many_hot_vec}
        else:
            code_dict[key][val] = many_hot_vec
    print(len(code_dict.keys()))
    return code_dict



def ico_dict_pop_encoder(cur):
    if cur.closed:
        print("Error: cursor is closed")

    list_of_codes = ico_map(cur)
    cur.execute("SELECT ADMISSIONS.SUBJECT_ID, ADMISSIONS.HADM_ID, ICD9_CODE \
    FROM diagnoses_icd INNER JOIN admissions ON diagnoses_icd.hadm_id = admissions.hadm_id \
    WHERE (ADMISSIONS.SUBJECT_ID IN (SELECT SUBJECT_ID from admissions GROUP BY SUBJECT_ID HAVING(COUNT(HADM_ID) = 1))) \
    ORDER BY ADMISSIONS.SUBJECT_ID")

    second_query = cur.fetchall()

    ico_dict = ico_code_ret_encoder(cur)

    for row in second_query:
        key1 = row[0]
        key2 = row[1]

        if row[2] is not None:
            code = row[2][0:3]
        else:
            continue

        idx = list_of_codes[code]
        if key1 in ico_dict:
            dict2 = ico_dict[key1]
            if key2 in dict2:
                cur_vect = ico_dict[key1][key2]
                cur_vect[idx] = 1
                ico_dict[key1][key2] = cur_vect


    return ico_dict








def main():
    current_connection = connect()
    current_cur = current_connection[1]
    ico_dict_pop_encoder(current_cur)
    close(current_connection[0], current_connection[1])

if __name__ == '__main__':
    main()

