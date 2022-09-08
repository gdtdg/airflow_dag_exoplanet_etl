from Repository import Repository


class InMemoryRepository(Repository):
    def __init__(self, init_data):
        self.store = init_data

    def add_all(self, items: list):
        self.store.extend(items)

    def get_all(self):
        return self.store
