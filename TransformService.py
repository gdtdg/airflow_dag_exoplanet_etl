import copy

from Repository import Repository


class TransformService:
    def __init__(self, in_repo: Repository, out_repo: Repository):
        self.in_repo = in_repo
        self.out_repo = out_repo

    def process(self):
        # Transform
        count = 1
        transformed_rows = []
        for row in self.in_repo.get_all():
            transformed_row = self.transform_row(row, count)
            transformed_rows.append(transformed_row)
            count += 1

        self.out_repo.add_all(transformed_rows)

    def transform_disc_pubdate_last_digit(self, row):
        try:
            if row['disc_pubdate'][-2:] == "00":
                delete_last_digit = row['disc_pubdate'][:-1] + "1"
                row['disc_pubdate'] = delete_last_digit
        except:
            pass

    def transform_pl_pubdate_date_format(self, row):
        if len(row['pl_pubdate']) != 7:
            row['pl_pubdate'] = row['pl_pubdate'][:7]

    def transform_releasedate_date_format(self, row):
        if len(row['releasedate']) != 10:
            row['releasedate'] = row['releasedate'][:10]

    def transform_rowupdate_date_format(self, row):
        if len(row['rowupdate']) == 0:
            row['rowupdate'] = row['releasedate']

    def add_rowid_to_row(self, row, count):
        if 'rowid' not in row:
            row['rowid'] = count

    def transform_row(self, row, count):
        new_row = copy.deepcopy(row)
        self.transform_disc_pubdate_last_digit(new_row)
        self.transform_pl_pubdate_date_format(new_row)
        self.transform_releasedate_date_format(new_row)
        self.transform_rowupdate_date_format(new_row)
        self.add_rowid_to_row(new_row, count)
        return new_row
