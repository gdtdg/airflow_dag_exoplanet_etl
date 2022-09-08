import copy


def transform_disc_pubdate_last_digit(row):
    try:
        if row['disc_pubdate'][-2:] == "00":
            delete_last_digit = row['disc_pubdate'][:-1] + "1"
            row['disc_pubdate'] = delete_last_digit
    except:
        pass


def transform_pl_pubdate_date_format(row):
    if len(row['pl_pubdate']) != 7:
        row['pl_pubdate'] = row['pl_pubdate'][:7]


def transform_releasedate_date_format(row):
    if len(row['releasedate']) != 10:
        row['releasedate'] = row['releasedate'][:10]


def transform_rowupdate_date_format(row):
    if len(row['rowupdate']) == 0:
        row['rowupdate'] = row['releasedate']


def add_rowid_to_row(row, count):
    if 'rowid' not in row:
        row['rowid'] = count


def transform_row(row, count):
    new_row = copy.deepcopy(row)
    transform_disc_pubdate_last_digit(new_row)
    transform_pl_pubdate_date_format(new_row)
    transform_releasedate_date_format(new_row)
    transform_rowupdate_date_format(new_row)
    add_rowid_to_row(new_row, count)

    return new_row
