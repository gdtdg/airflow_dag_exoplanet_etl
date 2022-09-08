from Repository import Repository
from library.etl_transform import transform_row


class TransformService:
    def __init__(self, in_repo: Repository, out_repo: Repository):
        self.in_repo = in_repo
        self.out_repo = out_repo

    def process(self):
        # Transform
        count = 1
        transformed_rows = []
        for row in self.in_repo.get_all():
            transformed_row = transform_row(row, count)
            transformed_rows.append(transformed_row)
            count += 1

        self.out_repo.add_all(transformed_rows)
