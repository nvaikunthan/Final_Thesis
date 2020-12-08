import pickle
import numpy as np


def map_merge(map1, map2):
    merged_map = {}
    idx = 0
    for key in map1:
        if key not in merged_map:
            merged_map[key] = idx
            idx += 1

    for key in map2:
        if key not in merged_map:
            merged_map[key] = idx
            idx += 1

    return merged_map


def cui_vectorizer(cui_dict, cui_map):
    vectorized_dict = {}
    vec_length = len(cui_map.keys())
    for key in cui_dict:
        visit_dict = cui_dict[key]
        for visit in visit_dict:
            notes = visit_dict[visit]
            dim1 = len(notes)
            if key not in vectorized_dict:
                vectorized_dict[key] = {visit: np.zeros((dim1, vec_length))}
            else:
                vectorized_dict[key][visit] = np.zeros((dim1, vec_length))
            for i, note in enumerate(notes):
                mat = vectorized_dict[key][visit]
                if note is not None:
                    for j in range(len(note)):
                        mat[i, cui_map[note[j]]] = 1
                vectorized_dict[key][visit] = mat

    return vectorized_dict


def check_print(dict, n):
    iter = 0
    for key in dict:
        val_dict = dict[key]
        print("Patient is: " + str(key))
        for val in val_dict:
            mat = val_dict[val]
            print("Visit is:" + str(val))
            for i in range(mat.shape[0]):
                for j in range(mat.shape[1]):
                    if mat[i, j] != 0:
                        print(i, j, mat[i, j])
        iter += 1
        if iter > n:
            break


def main():

    with open('cui_map_train.pickle', 'rb') as cui_map_train, open('cui_map_test.pickle', 'rb') as cui_map_test:
        train_map = pickle.load(cui_map_train)
        test_map = pickle.load(cui_map_test)
    with open('cui_dict_train.pickle', 'rb') as cui_dict_train, open('cui_dict_test.pickle', 'rb') as cui_dict_test:
        train_dict = pickle.load(cui_dict_train)
        test_dict = pickle.load(cui_dict_test)

    merged_map = map_merge(train_map, test_map)
    train_vectorized = cui_vectorizer(train_dict, merged_map)
    # test_vectorized = cui_vectorizer(train_dict, merged_map)


if __name__ == '__main__':
    main()

