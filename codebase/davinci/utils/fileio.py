import os
import pickle

from davinci.utils.logging import log

@log()
def force_folder_to_path(path):
    """Build folder path if it doesn't exist. Input
    assumes a full-path to a file, not a folder path itself.
    
    :param path: The file path to enforce folder existinence for.
    :type path: str
    """
    folders = "/".join(path.split('/')[:-1])
    if folders != '' and not os.path.exists(folders):
        os.makedirs(folders)

@log(unveil=True)
def save_to_pkl(obj, path):
    """Save object to pickle file.
    
    :param obj: The Python object to save.
    :type obj: any
    :param path: The file path to enforce folder existinence for.
    :type path: str
    :return: None
    """
    force_folder_to_path(path)
    with open(path, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

@log(unveil=True)
def load_pkl_file(path):
    """Load object from pickle file.
    
    :param path: The file path to enforce folder existinence for.
    :type path: str
    :return: Loaded object
    :rtype: Any
    """
    force_folder_to_path(path)
    with open(path, 'rb') as f:
        return pickle.load(f)