import os
import datetime
import fire


def print_files_modified_on_date(path: str, date: str = "2023-02-09"):
    """
    Recursively traverses a directory and prints information about each file
    that was modified on the specified date.

    Args:
        path (str): The path to the directory to traverse.
        date (str, optional): The date to filter files by, in the format
        "YYYY-MM-DD". Defaults to "2023-02-09".

    Returns:
        None: This function does not return anything. It simply prints
        information about each file to the console.
    """
    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    for root, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(root, file)
            last_modified_time = os.path.getmtime(full_path)
            last_modified_date = (datetime.datetime
                                  .fromtimestamp(last_modified_time).date())
            if last_modified_date == date_obj:
                print(f"{full_path} was last modified on {last_modified_date}")


if __name__ == '__main__':
    fire.Fire(print_files_modified_on_date)
