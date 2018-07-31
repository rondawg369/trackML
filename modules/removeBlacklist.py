import os
import re
import zipfile

import pandas as pd
from trackml.dataset import load_dataset, DTYPES


def removeBlacklist(path='../input/', file='blacklist_training.zip'):
    """ In the competition there are a number of particles that had non-physical behaviour.
    A blacklist was published of all non-physical particles.  This function removes the particles that are on the
    blacklist from the input data set.

    Parameters
    ----------------------------------
    path : str or pathlib.Path
        path to the directory containing the zipped data files

    file : str or pathlib.Path
        the blacklist filename
    """

    files = get_zips(path)
    print(files)
    files = ['train_sample.zip']  # only for testing
    blackList_ids, blackList_data = get_blacklist(path, file)
    print(blackList_ids)
    # Iterate through each of the zip files
    for f in files:
        # Iterate through each event in the file
        for event_id, hits, cells, particles, truth in load_dataset(path + f):
            print("Event ID:", event_id)
            if event_id in blackList_ids:
                print("This event contains blacklisted data")
                bl_hits, bl_particles = blackList_data[blackList_ids.index(event_id)]
                for hit in bl_hits['hit_id']:
                    hits.drop(hits[hits['hit_id'] == hit].index, inplace=True)
                    cells.drop(cells[cells['hit_id'] == hit].index, inplace=True)
                    truth.drop(truth[truth['hit_id'] == hit].index, inplace=True)
                for particle in bl_particles['particle_id']:
                    particles.drop(particles[particles['particle_id'] == particle].index, inplace=True)
                    truth.drop(truth[truth['particle_id'] == particle].index, inplace=True)
            hits.to_csv(path + 'train_sample/event00000' + str(event_id) + '-hits.csv', index=False)
            cells.to_csv(path + 'train_sample/event00000' + str(event_id) + '-cells.csv', index=False)
            particles.to_csv(path + 'train_sample/event00000' + str(event_id) + '-particles.csv', index=False)
            truth.to_csv(path + 'train_sample/event00000' + str(event_id) + '-truth.csv', index=False)


# TODO - save all of the newly created files back into their compressed folders
# TODO - cleanup all of the paths etc.

def get_zips(path):
    """ This function returns a list of the training zip files in the directory given by path

    Parameters
    ----------------------

    path : str or pathlib.Path
        This is the path to the directory containing the data files



    Returns
    -----------------------
    list
        Contains the file names for all of the data files
    """

    # List the input directory
    # reduce the list to only the training and test files
    regex = re.compile('.*^(train_[0-9]|test|train_sample)+.zip$')
    filenames = set(_ for _ in filter(regex.match, os.listdir(path)))
    return filenames


def get_blacklist(path, file):
    """ Taken directly from trackML library
    extract a sorted list of event file prefixes.
    Note: the file names may optionally have a directory prefix if they
    are derived from a zipfile, for example. Hence the regular expression
    can't be anchored at the beginning of the file name. """
    parts = ['hits', 'particles']
    with zipfile.ZipFile(path + file, mode='r') as z:
        regex = re.compile('.*event\d{9}-blacklist_[a-zA-Z]+.csv$')
        filenames = filter(regex.match, z.namelist())
        prefixes = set(_.split('-', 1)[0] for _ in filenames)
        prefixes = sorted(prefixes)
        event_ids = []
        data_list = []
        for p in prefixes:
            files = [z.open('{}-blacklist_{}.csv'.format(p, _), mode='r') for _ in parts]
            dtypes = [DTYPES[_] for _ in parts]
            data = tuple(pd.read_csv(f, header=0, index_col=False, dtype=d)
                         for f, d in zip(files, dtypes))
            regex = r".*event(\d+)"
            groups = re.findall(regex, p)
            event_ids.append(int(groups[0]))
            data_list.append(data)
    return event_ids, data_list
